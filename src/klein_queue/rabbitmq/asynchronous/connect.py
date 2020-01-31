# -*- coding: utf-8 -*-
# pylint: disable=import-error
'''
klein_queue.rabbitmq.async.connect
'''
import abc
import json
import logging
import random

import pika

from klein_config import config as common_config

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


def get_url_parameters(host):
    url = 'amqp://%s:%s@%s:%s/' % (
        common_config.get("rabbitmq.username"),
        common_config.get("rabbitmq.password"),
        host,
        common_config.get("rabbitmq.port"))
    connection_params = pika.URLParameters(url)
    connection_params._virtual_host = common_config.get("rabbitmq.vhost", "/")
    connection_params.socket_timeout = common_config.get(
        "rabbitmq.socket_timeout", 5)
    connection_params.heartbeat = common_config.get(
        "rabbitmq.heartbeat", 120)
    connection_params.blocked_connection_timeout = common_config.get(
        "rabbitmq.blocked_connection_timeout", 300)
    connection_params.retry_delay = common_config.get(
        "rabbitmq.retry_delay", 10)

    return connection_params


class Connection():
    '''
    Base connection class for publisher and consumer to inherit from
    '''

    def __init__(self, config):
        '''
        initialise connection parameters and reset internal vars
        '''

        self._connection_params = [get_url_parameters(host) for host in common_config.get("rabbitmq.host")]
        self._config = config
        self._connection = None
        self._channel = None
        self._closing = False

    def connect(self):
        '''
        create new connection to rabbitmq server
        '''
        random.shuffle(self._connection_params)
        for connection in self._connection_params:
            try:
                return pika.SelectConnection(connection, self.on_connection_open)

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
        self.add_on_connection_close_callback()
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        LOGGER.error('Connection open failed, reopening in 5 seconds: %s', err)
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def add_on_connection_close_callback(self):
        '''
        attaches on_connection_closed callback to close of connection
        '''
        LOGGER.debug('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
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
        if "prefetch" in self._config:
            prefetch = self._config["prefetch"]
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
        # pylint: disable=unused-argument
        '''
        if channel closed then log and close connection
        '''
        self._connection.close()

    def setup_exchanges(self):
        '''
        if exchanges configured then auto declare as fanout exchanges
        and bind callback for successful declaration
        if no exchanges configured then setup queues
        '''
        if "exchanges" in self._config:
            LOGGER.debug('Declaring exchanges %s', self._config["exchanges"])
            for ex in self._config['exchanges']:
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
        LOGGER.debug('Declaring queue %s', self._config["queue"])
        self._channel.queue_declare(callback=self.on_queue_declareok,
                                    queue=self._config["queue"],
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
        if "exchanges" in self._config:
            for ex in self._config['exchanges']:
                LOGGER.debug('Binding %s to %s', ex, self._config["queue"])
                ex_name = ex
                if isinstance(ex, dict):
                    if "name" not in ex:
                        raise RuntimeError(
                            "Invalid consumer configuration: %s" %
                            (json.dumps(ex)))
                    ex_name = ex["name"]

                self._channel.queue_bind(
                    self._config["queue"], ex_name, callback=self.on_bindok)
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
        LOGGER.debug('RabbitMQ acknowledged the cancellation of the activty')
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
        self._connection.ioloop.start()
        LOGGER.debug('Stopped')

    def close_connection(self):
        '''
        close connection
        '''
        LOGGER.debug('Closing connection')
        self._connection.close()

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
