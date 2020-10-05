# -*- coding: utf-8 -*-
import json
import logging
import functools
import threading
import queue
from .connect import Connection
from ..util import KleinQueueError

LOGGER = logging.getLogger(__name__)


class MessageWorker(threading.Thread):
    '''
    Message worker class
    '''

    def __init__(self, consumer):
        self._consumer = consumer
        self._closing = False
        super().__init__()

    def run(self):
        '''
        Loop and get messages from the message queue when they're available
        Pass message to consumers handler function
        If result returned from handler check to see if it is
        callable and execute otherwise acknowledge if not already done
        '''

        while not self._closing:
            try:
                # get a message from the queue
                (channel, basic_deliver, properties, body, auto_ack) = self._consumer._message_queue.get(True, 1)

                result = None

                nack_cb = functools.partial(self._consumer.negative_acknowledge_message, basic_deliver.delivery_tag, False, False)

                try:
                    result = self._consumer._handler_fn(json.loads(
                        body), basic_deliver=basic_deliver, properties=properties)

                except KleinQueueError as kqe:
                    kqe.body = json.dumps(body)
                    self._consumer.threadsafe_call(nack_cb)
                    raise kqe
                except Exception as err:
                    # pylint: disable=broad-except
                    # we want to catch all exceptions to make sure we send a nack and continue processing
                    self._consumer.threadsafe_call(nack_cb)
                    raise err

                if result is not None and callable(result):
                    result(self, channel, basic_deliver, properties)
                elif result is not False and not auto_ack:
                    LOGGER.info("Acknowledge on completion the message # %s", basic_deliver.delivery_tag)
                    ack_cb = functools.partial(self._consumer.acknowledge_message, basic_deliver.delivery_tag)
                    self._consumer.threadsafe_call(ack_cb)

            except queue.Empty:
                continue
            except Exception:
                continue


    def stop(self):
        self._closing = True


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
        self._message_queue = queue.Queue()
        self._workers = []

        LOGGER.info('Starting %d MessageWorker threads', workers)
        # spawn a number of worker threads (defaults to 1)
        for _ in range(workers):
            worker = MessageWorker(self)
            worker.start()
            self._workers.append(worker)

        super().__init__(config, key)

    def set_handler(self, handler_fn):
        self._handler_fn = handler_fn

    def start_activity(self):
        LOGGER.debug('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            on_message_callback=self.on_message, queue=self._queue["queue"])

    def negative_acknowledge_message(self, delivery_tag, multiple, requeue):
        '''
        Sends a negative acknowledgement (NACK)
        '''
        LOGGER.debug("Sending negative acknowledgement on message # %s, requeue: %s", delivery_tag, requeue)
        self._channel.basic_nack(delivery_tag, multiple, requeue)

    def on_message(self, channel, basic_deliver, properties, body):
        '''
        Handles an incoming message, adds it to the message queue to be processed by the worker threads
        channel: pika.Channel 
        basic_deliver: pika.spec.Basic.Deliver
        properties: pika.spec.BasicProperties 
        body: bytes
        '''

        LOGGER.debug('Received message # %s from %s: %s',
                     basic_deliver.delivery_tag, properties.app_id, body)

        auto_ack = self._queue.get("auto_acknowledge", False)

        # decode
        body = body.decode('utf-8')

        if auto_ack:
            LOGGER.info("Auto-acknowledge message # %s", basic_deliver.delivery_tag)
            self.acknowledge_message(basic_deliver.delivery_tag)

        self._message_queue.put((channel, basic_deliver, properties, body, auto_ack))
    
    def stop_activity(self):
        if self._channel:
            LOGGER.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self._consumer_tag, self.on_cancelok)

        # stop worker threads
        for worker in self._workers:
            worker.stop()
