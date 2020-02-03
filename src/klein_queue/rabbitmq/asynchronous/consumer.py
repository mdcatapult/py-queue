# -*- coding: utf-8 -*-
import json
import logging
import datetime

from klein_config import config as common_config
from .connect import Connection
from ..synchronous.publisher import Publisher

LOGGER = logging.getLogger(__name__)


class DoclibError(Exception):
    '''
    Doclib Error Class
    '''


class Consumer(Connection):
    '''
    Consumer class
    '''

    def __init__(self, config, handler_fn=None, error_queue=None):

        self._handler_fn = handler_fn
        self._consumer_tag = None
        if error_queue is not None:
            self._error_publisher = Publisher(common_config.get('error'))
        else:
            self._error_publisher = Publisher(error_queue)
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
        # pylint: disable=unused-argument
        '''
        On receipt of message check to see if auto acknowledge required
        Pass message to consumers handler function
        If result returned from handler check to see if it is
        callable and execute otherwise acknowledge if not already done
        '''
        LOGGER.debug('Received message # %s from %s: %s',
                     basic_deliver.delivery_tag, properties.app_id, body)

        auto_ack = common_config.get("consumer.auto_acknowledge", True)

        if auto_ack:
            self.acknowledge_message(basic_deliver.delivery_tag)

        result = None

        try:
            result = self._handler_fn(json.loads(
                body), basic_deliver=basic_deliver, properties=properties)
        except (json.decoder.JSONDecodeError, json.JSONDecodeError) as err:
            LOGGER.error("unable to process message %s : %s", body, str(err))

        except DoclibError:
            msg = {
                "consumer": properties.get("x-consumer"),
                "datetime": datetime.datetime.now(),
                "exception": properties.get('x-exception'),
                "message": properties.get("x-message"),
                "queue": properties.get("x-queue"),
                "payload": body,
                "trace": list(properties.get("x-stack-trace").split("\n")),
                "originalExchange": properties.get("x-original-exchange"),
                "originalRoutingKey": properties.get("x-original-routing-key")
            }

            self._error_publisher.publish(msg)
            self.acknowledge_message(basic_deliver.delivery_tag)

        if result is not None and callable(result):
            result(self, channel, basic_deliver, properties)
        elif result is not False or not auto_ack:
            self.acknowledge_message(basic_deliver.delivery_tag)

    def stop_activity(self):
        if self._channel:
            LOGGER.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self._consumer_tag, self.on_cancelok)
