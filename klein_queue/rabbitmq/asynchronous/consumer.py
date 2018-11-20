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

    def on_message(self, channel, basic_deliver, properties, body):
        # pylint: disable=unused-argument
        '''
        On receipt of message check to see if auto acknowledge required
        Pass message to consumers handler function
        If result returned from handler chek to see if it is 
        callable and execute otherwise acknowlege if not already done
        '''
        LOGGER.debug('Received message # %s from %s: %s',
                     basic_deliver.delivery_tag, properties.app_id, body)
        
        auto_ack = common_config.get("consumer.auto_acknowledge", True)

        if auto_ack:
            self.acknowledge_message(basic_deliver.delivery_tag)

        try:
            result = self._handler_fn(json.loads(
                body), basic_deliver=basic_deliver, properties=properties)
        except (json.decoder.JSONDecodeError, json.JSONDecodeError) as err:
            LOGGER.error("unable to process message %s : %s", body, str(err))

        if result is not None and callable(result):
            result(self, channel, basic_deliver, properties)
        elif result is True and not auto_ack:
            self.acknowledge_message(basic_deliver.delivery_tag)

    def stop_activity(self):
        if self._channel:
            LOGGER.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)
