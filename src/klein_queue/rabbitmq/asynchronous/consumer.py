# -*- coding: utf-8 -*-
import json
import logging
import functools
from threading import Thread
from .connect import Connection
from ..util import KleinQueueError

LOGGER = logging.getLogger(__name__)


class MessageHandlerThread(Thread):
    def __init__(self, consumer, channel, basic_deliver, properties, body):
        self._consumer = consumer
        self._channel = channel
        self._basic_deliver = basic_deliver
        self._properties = properties
        self._body = body

        super().__init__()

    def run(self):
        '''
        On receipt of message check to see if auto acknowledge required
        Pass message to consumers handler function
        If result returned from handler check to see if it is
        callable and execute otherwise acknowledge if not already done
        '''

        LOGGER.debug('Received message # %s from %s: %s',
                     self._basic_deliver.delivery_tag, self._properties.app_id, self._body)

        auto_ack = self._consumer._queue["auto_acknowledge"]
        ack_cb = functools.partial(self._consumer.acknowledge_message, self._basic_deliver.delivery_tag)

        if auto_ack:
            LOGGER.info("Auto-acknowledge message # %s", self._basic_deliver.delivery_tag)
            self._consumer.threadsafe_call(ack_cb)

        result = None

        try:
            result = self._consumer._handler_fn(json.loads(
                self._body), basic_deliver=self._basic_deliver, properties=self._properties)
        except (json.decoder.JSONDecodeError, json.JSONDecodeError) as err:
            LOGGER.error("unable to process message %s : %s", self._body, str(err))
        except KleinQueueError as kqe:
            kqe.body = json.dumps(self._body)
            raise kqe

        if result is not None and callable(result):
            result(self, self._channel, self._basic_deliver, self._properties)
        elif result is not False and not auto_ack:
            LOGGER.info("Acknowledge on completion the message # %s", self._basic_deliver.delivery_tag)
            self._consumer.threadsafe_call(ack_cb)


class Consumer(Connection):
    '''
    Consumer class
    '''

    def __init__(self, config, key, handler_fn=None):
        self._queue = config.get(key)
        self._config = config
        self._handler_fn = handler_fn
        self._handler_thread = None
        self._consumer_tag = None

        super().__init__(config, key)

    def set_handler(self, handler_fn):
        self._handler_fn = handler_fn

    def start_activity(self):
        LOGGER.debug('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            on_message_callback=self.on_message, queue=self._queue["queue"])

    def threadsafe_call(self, cb):
        '''
        execute a callback in the same context as the ioloop
        '''
        self._connection.ioloop.call_later(0, cb)

    def on_message(self, channel, basic_deliver, properties, body):
        '''
        Checks if we're ready to consume another message, and if so starts a MessageHandlerThread to do it
        channel: pika.Channel 
        basic_deliver: pika.spec.Basic.Deliver
        properties: pika.spec.BasicProperties 
        body: bytes
        '''

        if self._handler_thread is None or not self._handler_thread.is_alive():
            self._handler_thread = MessageHandlerThread(self, channel, basic_deliver, properties, body)
            self._handler_thread.start()
        else:
            # Requeue the message if we're not ready for another
            self.nack_message(basic_deliver.delivery_tag, False, True)
    
    def stop_activity(self):
        if self._channel:
            LOGGER.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self._consumer_tag, self.on_cancelok)
