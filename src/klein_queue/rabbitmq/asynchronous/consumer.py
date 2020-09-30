# -*- coding: utf-8 -*-
import json
import logging
import functools
import threading
from queue import Queue
from .connect import Connection
from ..util import KleinQueueError

LOGGER = logging.getLogger(__name__)

class MessageWorker(threading.Thread):
    def __init__(self, consumer):
        self._consumer = consumer
        super().__init__()

    def run(self):
        '''
        On receipt of message check to see if auto acknowledge required
        Pass message to consumers handler function
        If result returned from handler check to see if it is
        callable and execute otherwise acknowledge if not already done
        '''

        while True:
            # get a message from the queue (blocking until one is available)
            (channel, basic_deliver, properties, body, auto_ack) = self._consumer._message_queue.get(True)

            result = None

            try:
                result = self._consumer._handler_fn(json.loads(
                    body), basic_deliver=basic_deliver, properties=properties)
            except (json.decoder.JSONDecodeError, json.JSONDecodeError) as err:
                LOGGER.error("unable to process message %s : %s", body, str(err))
            except KleinQueueError as kqe:
                kqe.body = json.dumps(body)
                raise kqe

            if result is not None and callable(result):
                result(self, channel, basic_deliver, properties)
            elif result is not False and not auto_ack:
                LOGGER.info("Acknowledge on completion the message # %s", basic_deliver.delivery_tag)
                ack_cb = functools.partial(self._consumer.acknowledge_message, basic_deliver.delivery_tag)
                self._consumer.threadsafe_call(ack_cb)


class Consumer(Connection):
    '''
    Consumer class
    '''

    def __init__(self, config, key, handler_fn=None, workers=1):
        self._queue = config.get(key)
        self._config = config
        self._handler_fn = handler_fn
        self._handler_thread = None
        self._consumer_tag = None
        self._message_queue = Queue()
        # spawn a number of worker threads (defaults to 1)
        for _ in range(workers):
            worker = MessageWorker(self)
            worker.start()

        super().__init__(config, key)

    def set_handler(self, handler_fn):
        self._handler_fn = handler_fn

    def start_activity(self):
        LOGGER.debug('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            on_message_callback=self.on_message, queue=self._queue["queue"])

    def on_message(self, channel, basic_deliver, properties, body):
        '''
        Checks if we're ready to consume another message, and if so starts a MessageHandlerThread to do it
        channel: pika.Channel 
        basic_deliver: pika.spec.Basic.Deliver
        properties: pika.spec.BasicProperties 
        body: bytes
        '''

        LOGGER.debug('Received message # %s from %s: %s',
                     basic_deliver.delivery_tag, properties.app_id, body)

        auto_ack = self._queue.get("auto_acknowledge", True)

        if auto_ack:
            LOGGER.info("Auto-acknowledge message # %s", basic_deliver.delivery_tag)
            self.acknowledge_message(basic_deliver.delivery_tag)

        self._message_queue.put((channel, basic_deliver, properties, body, auto_ack))
    
    def stop_activity(self):
        if self._channel:
            LOGGER.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self._consumer_tag, self.on_cancelok)
