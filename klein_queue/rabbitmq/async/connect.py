# -*- coding: utf-8 -*-
from klein_config import config as common_config
import pika
import argparse
import logging

parser = argparse.ArgumentParser()
parser.add_argument("--debug", help="enable debug", action="store_true")
args, unknown = parser.parse_known_args()

LOGGER = logging.getLogger(__name__)


def blocking(config):
    url = 'amqp://%s:%s@%s:%s/?backpressure_detection=t' % (
        config["username"],
        config["password"],
        config["host"],
        config["port"])
    params = pika.URLParameters(url)
    params.socket_timeout = 5
    LOGGER.debug("RabbitMQ - Connecting: %s ")
    return pika.BlockingConnection(params)


class Connection(object):

    def __init__(self, config):
        self._url = 'amqp://%s:%s@%s:%s/?backpressure_detection=t' % (
            common_config["rabbitmq"]["username"],
            common_config["rabbitmq"]["password"],
            common_config["rabbitmq"]["host"],
            common_config["rabbitmq"]["port"])

        self._config = config
        self._connection = None
        self._channel = None
        self._closing = False
        self._connection_params = pika.URLParameters(self._url)
        self._connection_params.socket_timeout = 5

    def connect(self):
        LOGGER.debug('Connecting to %s', self._url)
        return pika.SelectConnection(self._connection_params,
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        LOGGER.debug('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        LOGGER.debug('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            if args.debug:
                LOGGER.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                               reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        self._connection.ioloop.stop()
        if not self._closing:
            self._connection = self.connect()
            self._connection.ioloop.start()

    def open_channel(self):
        LOGGER.debug('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.debug('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchanges()

    def add_on_channel_close_callback(self):
        LOGGER.debug('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        if args.debug:
            LOGGER.warning('Channel %i was closed: (%s) %s', channel, reply_code, reply_text)
        self._connection.close()

    def setup_exchanges(self):
        if "exchanges" in self._config:
            LOGGER.debug('Declaring exchanges %s', self._config["exchanges"])
            for ex in self._config['exchanges'].split(','):
                self._channel.exchange_declare(self.on_exchange_declareok, ex, 'fanout')
        else:
            self.setup_queue()

    def on_exchange_declareok(self, unused_frame):
        LOGGER.debug('Exchange declared')
        self.setup_queue()

    def setup_queue(self):
        LOGGER.debug('Declaring queue %s', self._config["queue"])
        self._channel.queue_declare(self.on_queue_declareok,
                                    queue=self._config["queue"],
                                    durable=True,
                                    exclusive=False,
                                    auto_delete=False)

    def on_queue_declareok(self, method_frame):
        if "exchanges" in self._config:
            for ex in self._config['exchanges'].split(','):
                LOGGER.debug('Binding %s to %s', ex, self._config["queue"])
                self._channel.queue_bind(self.on_bindok, self._config["queue"], ex)
        else:
            self.start_activity()

    def on_bindok(self, unused_frame):
        LOGGER.debug('Queue bound')
        self.start_activity()

    def acknowledge_message(self, delivery_tag):
        LOGGER.debug('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def on_cancelok(self, unused_frame):
        LOGGER.debug('RabbitMQ acknowledged the cancellation of the activty')
        self.close_channel()

    def add_on_cancel_callback(self):
        LOGGER.debug('Adding activity cancellation callback')
        self._channel.add_on_cancel_callback(self.on_activity_cancelled)

    def on_activity_cancelled(self, method_frame):
        LOGGER.debug('Activity was cancelled remotely, shutting down: %r', method_frame)
        if self._channel:
            self._channel.close()

    def close_channel(self):
        LOGGER.debug('Closing the channel')
        self._channel.close()

    def run(self):
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
        LOGGER.debug('Stopping')
        self._closing = True
        self.stop_activity()
        self._connection.ioloop.start()
        LOGGER.debug('Stopped')

    def close_connection(self):
        LOGGER.debug('Closing connection')
        self._connection.close()

    def stop_activity(self):
        LOGGER.debug('You must define a stop_activity method')

    def start_activity(self):
        LOGGER.debug('You must define a start_activity method')
