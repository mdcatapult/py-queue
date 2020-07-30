# -*- coding: utf-8 -*-
# pylint: disable=import-error
'''
klein_queue.rabbitmq.sync.connect
'''
import json
import logging

import pika

from ..util import get_url_parameters

LOGGER = logging.getLogger(__name__)


class Connection:
    '''
    Base connection for consumers and publisher to inherit
    '''

    def __init__(self, config, key):
        '''
        initialise connection parameters and reset internal vars
        '''
        self._connection_params = get_url_parameters(config)
<<<<<<< HEAD
        self._queue = config.get(key)
=======
>>>>>>> 0fe1c17bbb77ad97d72d7616eaadafbf82e2e89b
        self._config = config
        self._connection = None
        self._channel = None
        self._closing = False

    def connect(self):
        '''
        create new connection to rabbitmq server
        '''

        for connection in self._connection_params:
            try:
                if not self._connection or self._connection.is_closed:
                    self._connection = pika.BlockingConnection(connection)
                    self.open_channel()
                    self.setup_exchanges()
                    break

            except pika.exceptions.ConnectionClosedByBroker:
                LOGGER.debug("Connection was closed by broker")
                # Uncomment this to make the example not attempt recovery
                # from server-initiated connection closure, including
                # when the node is stopped cleanly
                #
                # break
                continue

            except pika.exceptions.AMQPChannelError as err:
                LOGGER.debug("Caught a channel error: %s, stopping...", err)
                break

            # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError:
                LOGGER.debug("Connection was closed, retrying...")
                continue

    def open_channel(self):
        '''
        open channel to rabbitmq
        '''
        LOGGER.debug('Creating a new channel')
        self._channel = self._connection.channel()

        prefetch = 1
        if "prefetch" in self._queue:
            prefetch = self._queue["prefetch"]
        self._channel.basic_qos(prefetch_count=prefetch)

    def setup_exchanges(self):
        '''
        if exchanges configured then auto declare as fanout exchanges
        then setup queues
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
                self._channel.exchange_declare(ex_name, ex_type)
        if self._config.get("rabbitmq.create_queue_on_connect", True) and not (
<<<<<<< HEAD
                "create_on_connect" in self._queue and not self._queue["create_on_connect"]):
=======
                "create_on_connect" in self._config and not self._config["create_on_connect"]):
>>>>>>> 0fe1c17bbb77ad97d72d7616eaadafbf82e2e89b
            self.setup_queue()

    def setup_queue(self):
        '''
        declare queue with rabbitmq, ensuring durability
        '''
        create_queue = self._config.get("rabbitmq.create_queue_on_connect", True) and not (
<<<<<<< HEAD
                "create_on_connect" in self._queue and not self._queue["create_on_connect"])
        if create_queue and "queue" in self._queue and self._queue["queue"] is not False:
            LOGGER.debug('Declaring queue %s', self._queue["queue"])
            self._channel.queue_declare(queue=self._queue["queue"],
=======
                "create_on_connect" in self._config and not self._config["create_on_connect"])
        if create_queue and "queue" in self._config and self._config["queue"] is not False:
            LOGGER.debug('Declaring queue %s', self._config["queue"])
            self._channel.queue_declare(queue=self._config["queue"],
>>>>>>> 0fe1c17bbb77ad97d72d7616eaadafbf82e2e89b
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
                self._channel.queue_bind(self._queue["queue"], ex_name)

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
