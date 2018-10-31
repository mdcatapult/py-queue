# -*- coding: utf-8 -*-
from .connect import Connection
import logging
import pika
import pika.exceptions
import argparse
import json

LOGGER = logging.getLogger(__name__)


class Publisher(Connection):

    def __init__(self, config):
        super().__init__(config)

    def __call__(self, message):
        self.publish(message)

    def publish(self, message):
        try:
            self.publish_message(message)
        except pika.exceptions.ConnectionClosed:
            LOGGER.debug('reconnecting to queue')
            self.connect()
            self.publish_message(message)

    def publish_message(self, message):
        if self._closing:
            LOGGER.debug('Publisher currently stopping, unable to publish messages at this time')
            return

        # properties = pika.BasicProperties(app_id='klein-consumer',
        #                                   content_type='application/json',
        #                                   headers=message)

        routing_key = ''
        exchange = ''

        if ("queue" not in self._config or self._config["queue"] is False) and "exchange" in self._config:
            exchange = self._config["exchange"]
        elif "queue" in self._config:
            routing_key = self._config["queue"]
        else:
            print('Unable to publish message no valid routing key or exchange defined')
            return

        LOGGER.debug('Publishing message %s to queue "%s"', json.dumps(message), routing_key if len(routing_key)>0 else exchange)
        self._channel.publish(exchange, routing_key, json.dumps(message))
