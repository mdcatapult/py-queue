# -*- coding: utf-8 -*-
# pylint: disable=import-error
'''
klein_queue.rabbitmq.async.connect
'''
import abc
import json
import logging

import pika
import pika.exceptions

from ..util import get_url_parameters

LOGGER = logging.getLogger(__name__)


def blocking(config):
    '''
    create blocking connection with supplied config
    '''
    url = 'amqp://%s:%s@%s:%s/' % (
        config["username"],
        config["password"],
        config["host"],
        config["port"])
    params = pika.URLParameters(url)
    params.socket_timeout = 5
    LOGGER.debug("RabbitMQ - Connecting: %s ")
    return pika.BlockingConnection(params)


class Connection:
    '''
    Base connection class for publisher and consumer to inherit from
    '''

    def __init__(self, config, key):
        '''
        initialise connection parameters and reset internal vars
        '''
        self._connection_params = get_url_parameters(config)
        self._config = config
        self._queue = config.get(key)
        self._connection = None
        self._channel = None
        self._closing = False

    def connect(self):
        '''
        create new connection to rabbitmq server
        '''

        for connection in self._connection_params:
            try:
                return pika.SelectConnection(
                    parameters=connection,
                    on_open_callback=self.on_connection_open,
                    on_open_error_callback=self.on_connection_open_error,
                    on_close_callback=self.on_connection_closed,
                )

            except pika.exceptions.ConnectionClosedByBroker:
                print('Connection closed by broker')
                continue

            except pika.exceptions.AMQPChannelError as err:
                print("Caught a channel error: {}, stopping...".format(err))
                break

            # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError:
                print("Connection was closed, retrying...")
                continue

    def on_connection_open(self, unused_connection):
        # pylint: disable=unused-argument
        '''
        callback passed to connection
        triggered on successfully opening connection
        opens channel
        '''

        LOGGER.debug('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        LOGGER.error('Connection open failed, reopening in 5 seconds: %s', err)
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(self, connection, reason):
        # pylint: disable=unused-argument
        '''
        when connection closed intentionally stop the ioloop
        otherwise attempt to reconnect
        '''
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._connection.ioloop.call_later(5, self.reconnect)

    def reconnect(self):
        '''
        stop ioloop and if not intentional reconnect immediately
        '''
        LOGGER.debug('Reconnect channel: current closing state is %s', self._closing)
        self._connection.ioloop.stop()
        if not self._closing:
            self._connection = self.connect()
            self._connection.ioloop.start()

    def open_channel(self):
        '''
        open channel to rabbitmq and bind callback
        '''
        LOGGER.debug('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)


    def on_channel_open(self, channel):
        '''
        on successful open of channel then bind close callback
        configures qos
        also setup exchanges
        '''
        LOGGER.debug('Channel opened')
        self._channel = channel

        prefetch = 1
        if "prefetch" in self._queue:
            prefetch = self._queue["prefetch"]
        self._channel.basic_qos(prefetch_count=prefetch)

        self.add_on_channel_close_callback()
        self.setup_exchanges()

    def add_on_channel_close_callback(self):
        '''
        bind on close channel callback
        '''
        LOGGER.debug('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        '''
        if channel closed then log and close connection
        '''
        LOGGER.info("connection to channel %s closed because %s", channel, reason)
        try:
            self._connection.close()
        except pika.exceptions.ConnectionWrongStateError:
            LOGGER.warning("connection to channel %s is already closed or is closing", channel)

    def setup_exchanges(self):
        '''
        if exchanges configured then auto declare as fanout exchanges
        and bind callback for successful declaration
        if no exchanges configured then setup queues
        '''
        if "exchanges" in self._queue:
            LOGGER.debug('Declaring exchanges %s', self._queue["exchanges"])
            for ex in self._queue['exchanges']:
                ex_name = ex
                ex_type = 'fanout'
                if isinstance(ex, dict):
                    if "name" not in ex or "type" not in ex:
                        raise RuntimeError(
                            "Invalid consumer configuration: %s" %
                            (json.dumps(ex)))
                    ex_name = ex["name"]
                    ex_type = ex["type"]

                self._channel.exchange_declare(
                    ex_name, ex_type, callback=self.on_exchange_declareok)
        else:
            LOGGER.debug('No exchanges to declare so setting up queue')
            self.setup_queue()

    def on_exchange_declareok(self, unused_frame):
        # pylint: disable=unused-argument
        '''
        on sucessful declaration of exchange then setup queue
        '''
        LOGGER.debug('Exchange declared')
        self.setup_queue()

    def setup_queue(self):
        '''
        declare queue with rabbitmq, ensuring durability
        '''

        create_queue = self._config.get("rabbitmq.create_queue_on_connect", True) and not (
                "create_on_connect" in self._queue and not self._queue["create_on_connect"])
        if create_queue:
            LOGGER.debug('Declaring queue %s', self._queue["queue"])
            self._channel.queue_declare(queue=self._queue["queue"],
                                        callback=self.on_queue_declareok,
                                        durable=True,
                                        exclusive=False,
                                        auto_delete=False,
                                        arguments={
                                            "queue-mode": "lazy"
                                        })

    def on_queue_declareok(self, method_frame):
        # pylint: disable=unused-argument
        '''
        if exchanges configured then bind queue to exchange
        '''
        if "exchanges" in self._queue:
            for ex in self._queue['exchanges']:
                LOGGER.debug('Binding %s to %s', ex, self._queue["queue"])
                ex_name = ex
                if isinstance(ex, dict):
                    if "name" not in ex:
                        raise RuntimeError(
                            "Invalid consumer configuration: %s" %
                            (json.dumps(ex)))
                    ex_name = ex["name"]

                self._channel.queue_bind(
                    self._queue["queue"], ex_name, callback=self.on_bindok)
        else:
            self.start_activity()

    def on_bindok(self, unused_frame):
        # pylint: disable=unused-argument
        '''
        start consuming using abstract method from descendent
        '''
        LOGGER.debug('Queue bound')
        self.start_activity()

    def acknowledge_message(self, delivery_tag):
        '''
        ack message
        '''
        LOGGER.debug('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def on_cancelok(self, unused_frame):
        # pylint: disable=unused-argument
        '''
        if cancelled then close channel
        '''
        LOGGER.debug('RabbitMQ acknowledged the cancellation of the activity')
        self.close_channel()

    def add_on_cancel_callback(self):
        '''
        bind callback to cancel
        '''
        LOGGER.debug('Adding activity cancellation callback')
        self._channel.add_on_cancel_callback(self.on_activity_cancelled)

    def on_activity_cancelled(self, method_frame):
        '''
        close channel
        '''
        LOGGER.debug(
            'Activity was cancelled remotely, shutting down: %r', method_frame)
        if self._channel:
            self.close_channel()

    def close_channel(self):
        '''
        close channel
        '''
        LOGGER.debug('Closing the channel')
        self._channel.close()

    def run(self):
        '''
        start connection and ioloop
        '''
        self._connection = self.connect()
        self._connection.ioloop.start()
        while not self._closing:
            try:
                self._connection = self.connect()
                self._connection.ioloop.start()

            except KeyboardInterrupt:
                self.stop()
                if (self._connection is not None and
                        not self._connection.is_closed):
                    # Finish closing
                    self._connection.ioloop.start()

        LOGGER.info('Stopped')

    def stop(self):
        '''
        cleanly stop and close connection
        '''
        LOGGER.debug('Stopping')
        self._closing = True
        self.stop_activity()
        self._connection.ioloop.stop()
        LOGGER.debug('Stopped')

    def close_connection(self):
        '''
        close connection
        '''
        LOGGER.debug('Closing connection')
        self._connection.close()

    def threadsafe_call(self, cb):
        '''
        Runs a callback in the context of the IO loop
        :param cb: a callback function
        '''
        self._connection.ioloop.call_later(0, cb)

    @abc.abstractmethod
    def stop_activity(self):
        '''
        You must define a stop_activity method
        '''
        return

    @abc.abstractmethod
    def start_activity(self):
        '''
        You must define a start_activity method
        '''
        return
