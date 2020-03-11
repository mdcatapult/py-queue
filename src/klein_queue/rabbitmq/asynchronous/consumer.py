# -*- coding: utf-8 -*-
import json
import logging
import datetime
from traceback import format_tb

import pika

from klein_config import config as common_config
from .connect import Connection
from ..synchronous.publisher import Publisher
from ..util import KleinQueueError

LOGGER = logging.getLogger(__name__)


class Consumer(Connection):
    '''
    Consumer class
    '''

    def __init__(self, config, handler_fn=None):
        self._local_config = config
        self._handler_fn = handler_fn
        self._consumer_tag = None
        if config.get('error') is None:
            self._error_publisher = Publisher(common_config.get('error'))
        else:
            self._error_publisher = Publisher(config.get('error'))
        self._error_publisher.connect()

        super().__init__(config)

    def set_handler(self, handler_fn):
        self._handler_fn = handler_fn

    def start_activity(self):
        LOGGER.debug('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            on_message_callback=self.on_message, queue=common_config.get("consumer.queue"))

    def on_message(self, channel, basic_deliver, properties, body):
        '''
        On receipt of message check to see if auto acknowledge required
        Pass message to consumers handler function
        If result returned from handler check to see if it is
        callable and execute otherwise acknowledge if not already done
        channel: pika.Channel 
        basic_deliver: pika.spec.Basic.Deliver
        properties: pika.spec.BasicProperties 
        body: bytes
        '''
        LOGGER.debug('Received message # %s from %s: %s',
                     basic_deliver.delivery_tag, properties.app_id, body)

        auto_ack = common_config.get("consumer.auto_acknowledge", True)

        if auto_ack:
            LOGGER.info("Auto-acknowledge message # %s", basic_deliver.delivery_tag)
            self.acknowledge_message(basic_deliver.delivery_tag)

        result = None

        try:
            result = self._handler_fn(json.loads(
                body), basic_deliver=basic_deliver, properties=properties)
        except (json.decoder.JSONDecodeError, json.JSONDecodeError) as err:
            LOGGER.error("unable to process message %s : %s", body, str(err))

        except KleinQueueError as kqe:
            excptn = kqe
            if kqe.__cause__ is not None and isinstance(kqe.__cause__, Exception):
                excptn = kqe.__cause__

            headers = {
                "x-consumer": self._local_config.get("name", "Unknown"),
                "x-datetime": datetime.datetime.now().isoformat(),
                "x-exception": str(type(excptn)),
                "x-message": str(excptn),
                "x-queue": self._local_config["queue"],
                "x-stack-trace": "\n".join(format_tb(excptn.__traceback__))
            }

            self._error_publisher.publish(
                body.decode("utf-8"),
                pika.BasicProperties(headers=headers, content_type='application/json')
            )

        if result is not None and callable(result):
            result(self, channel, basic_deliver, properties)
        elif result is not False and not auto_ack:
            LOGGER.info("Acknowledge on completion the message # %s", basic_deliver.delivery_tag)
            self.acknowledge_message(basic_deliver.delivery_tag)

    def stop_activity(self):
        if self._channel:
            LOGGER.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self._consumer_tag, self.on_cancelok)
