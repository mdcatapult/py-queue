# -*- coding: utf-8 -*-
# pylint: disable=import-error
'''
klein_queue.rabbitmq.sync.connect
'''
import logging
import json
import pika
from klein_config import config as common_config


LOGGER = logging.getLogger(__name__)


class Connection():
    '''
    Base connection for consumers and publisher to inherit
    '''

    def __init__(self, config):
        '''
        initialise connection parameters and reset internal vars
        '''
        self._url = 'amqp://%s:%s@%s:%s/?backpressure_detection=t' % (
            common_config.get("rabbitmq.username"),
            common_config.get("rabbitmq.password"),
            common_config.get("rabbitmq.host"),
            common_config.get("rabbitmq.port"))

        self._config = config
        self._connection = None
        self._channel = None
        self._closing = False
        self._connection_params = pika.URLParameters(self._url)
        self._connection_params.socket_timeout = common_config.get(
            "rabbitmq.socket_timeout", 5)
        self._connection_params.heartbeat = common_config.get(
            "rabbitmq.heartbeat", 120)
        self._connection_params.blocked_connection_timeout = common_config.get(
            "rabbitmq.blocked_connection_timeout", 300)
        self._connection_params.retry_delay = common_config.get(
            "rabbitmq.retry_delay", 10)

    def connect(self):
        '''
        create new connection to rabbitmq server
        '''
        if not self._connection or self._connection.is_closed:
            LOGGER.debug('Connecting to %s', self._url)
            self._connection = pika.BlockingConnection(self._connection_params)
            self.open_channel()
            self.setup_exchanges()

    def open_channel(self):
        '''
        open channel to rabbitmq
        '''
        LOGGER.debug('Creating a new channel')
        self._channel = self._connection.channel()

        prefetch = 1
        if "prefetch" in self._config:
            prefetch = self._config["prefetch"]
        self._channel.basic_qos(prefetch_count=prefetch)

    def setup_exchanges(self):
        '''
        if exchanges configured then auto declare as fanout exchanges
        then setup queues
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
                self._channel.exchange_declare(ex_name, ex_type)
        self.setup_queue()

    def setup_queue(self):
        '''
        declare queue with rabbitmq, ensuring durability
        '''
        if "queue" in self._config and self._config["queue"] is not False:
            LOGGER.debug('Declaring queue %s', self._config["queue"])
            self._channel.queue_declare(queue=self._config["queue"],
                                        durable=True,
                                        exclusive=False,
                                        auto_delete=False,
                                        arguments={
                                            "queue-mode": "lazy"
            })
            self.bind_to_exchange()

    def bind_to_exchange(self):
        '''
        If exchanges configured then bind the queue to it
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
                self._channel.queue_bind(self._config["queue"], ex_name)

    def acknowledge_message(self, delivery_tag):
        '''
        ack message
        '''
        LOGGER.debug('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def close_channel(self):
        '''
        close channel
        '''
        LOGGER.debug('Closing the channel')
        self._channel.close()

    def close_connection(self):
        '''
        close connection
        '''
        LOGGER.debug('Closing connection')
        self._connection.close()

    def stop(self):
        '''
        cleanly stop connection
        '''
        LOGGER.debug('Stopping')
        self._closing = True
        self.close_channel()
        self.close_connection()
        LOGGER.debug('Stopped')