# -*- coding: utf-8 -*-
import json
import logging
from klein_config import config as common_config
from .connect import Connection


LOGGER = logging.getLogger(__name__)


class Consumer(Connection):
    '''
    Consumer class
    '''

    def __init__(self, config, handler_fn):
        self._handler_fn = handler_fn
        self._consumer_tag = None
        super().__init__(config)

    def start_activity(self):
        LOGGER.debug('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self.on_message, queue=common_config.get("consumer.queue"))

    def on_message(self, unused_channel, basic_deliver, properties, body):
        # pylint: disable=unused-argument
        LOGGER.debug('Received message # %s from %s: %s',
                     basic_deliver.delivery_tag, properties.app_id, body)
        self.acknowledge_message(basic_deliver.delivery_tag)

        try:
            self._handler_fn(json.loads(
                body), basic_deliver=basic_deliver, properties=properties)
        except (json.decoder.JSONDecodeError, json.JSONDecodeError) as err:
            LOGGER.error("unable to process message %s : %s", body, str(err))

    def stop_activity(self):
        if self._channel:
            LOGGER.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)
