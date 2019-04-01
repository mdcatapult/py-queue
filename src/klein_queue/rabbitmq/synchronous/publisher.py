# -*- coding: utf-8 -*-
# pylint: disable=import-error
'''
klein_queue.rabbitmq.sync.publisher
'''
import logging
import json
import pika
import pika.exceptions
from .connect import Connection

LOGGER = logging.getLogger(__name__)


class Publisher(Connection):
    '''
    Synchronous publisher,
    good for publishing single messages programatically
    '''

    def __call__(self, message):
        self.publish(message)

    def publish(self, message):
        try:
            self.publish_message(message)
        except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed) as err:
            LOGGER.debug('reconnecting to queue')
            print('reconnecting to queue', err)
            self.connect()
            self.publish_message(message)

    def publish_message(self, message):
        if self._closing:
            LOGGER.debug(
                'Publisher currently stopping, unable to publish messages at this time')
            return

        # properties = pika.BasicProperties(app_id='klein-consumer',
        #                                   content_type='application/json',
        #                                   headers=message)

        routing_key = ''
        exchange = ''

        if "exchange" in self._config:
            exchange = self._config["exchange"]
        
        if "queue" in self._config:
            routing_key = self._config["queue"]

        if not exchange and not routing_key:
            print('Unable to publish message no valid routing key or exchange defined')
            return

        LOGGER.debug('Publishing message %s to queue "%s"', json.dumps(
            message), routing_key if routing_key else exchange)

        self._channel.publish(exchange, routing_key, json.dumps(message))