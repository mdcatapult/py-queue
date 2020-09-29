# -*- coding: utf-8 -*-
# pylint: disable=import-error
import json
import logging
from queue import Queue
import pika

from .connect import Connection

LOGGER = logging.getLogger(__name__)


class Publisher(Connection):

    def __init__(self, config, key):
        self._publish_interval = config["publishInterval"] if "publishInterval" in config else 1
        self._messages = Queue()
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._stopping = False
        super().__init__(config, key)

    def start_activity(self):
        LOGGER.debug('Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()
        self.schedule_next_message()

    def stop_activity(self):
        self._stopping = True
        self.close_channel()
        self.close_connection()

    def enable_delivery_confirmations(self):
        LOGGER.debug('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOGGER.debug('Received %s for delivery tag: %i',
                     confirmation_type,
                     method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        LOGGER.debug('Published %i messages, %i have yet to be confirmed, '
                     '%i were acked and %i were nacked',
                     self._message_number, len(self._deliveries),
                     self._acked, self._nacked)

    def schedule_next_message(self):
        if self._stopping:
            return

        LOGGER.debug('Scheduling next message for %0.1f seconds',
                     self._publish_interval)
        self._connection.ioloop.call_later(self._publish_interval,
                                           self.publish_message)


    def publish_message(self):
        if self._stopping:
            LOGGER.debug(
                'Publisher currently stopping, unable to publish messages at this time')
            return

        if self._messages.empty():
            # no messages to publish... do nothing
            return

        (message, properties) = self._messages.get(False)

        LOGGER.debug('Publishing message to queue %s', self._queue["queue"])
        self._channel.basic_publish('', self._queue["queue"],
                                    json.dumps(message),
                                    properties)

        self._message_number += 1
        self._deliveries.append(self._message_number)
        LOGGER.debug('Published message # %i', self._message_number)
        self.schedule_next_message()

    def add(self, message, properties=None):
        LOGGER.debug(
            'Adding message to internal stack ready for publishing')
        self._messages.put((message, properties))
