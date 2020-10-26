# -*- coding: utf-8 -*-
import json
import logging
import functools
import threading
import queue
from .connect import _Connection
from ..errors import KleinQueueError

LOGGER = logging.getLogger(__name__)


class _MessageWorker(threading.Thread):
    """
    Message worker class
    """

    def __init__(self, consumer):
        self._consumer = consumer
        self._closing = False
        super().__init__()

    def run(self):
        """
        Loop and get messages from the message queue when they're available
        Pass message to consumers handler function
        If result returned from handler check to see if it is
        callable and execute otherwise acknowledge if not already done
        """

        while not self._closing:
            try:
                # get a message from the queue
                (channel, basic_deliver, properties, body, auto_ack) = self._consumer._message_queue.get(True, 1)

                try:
                    self._consumer.handler_fn(json.loads(
                        body), basic_deliver=basic_deliver, properties=properties)
                    LOGGER.info("Acknowledge on completion the message # %s", basic_deliver.delivery_tag)
                    ack_cb = functools.partial(self._consumer.acknowledge_message, basic_deliver.delivery_tag)
                    self._consumer.threadsafe_call(ack_cb)
                except (KleinQueueError, json.decoder.JSONDecodeError, json.JSONDecodeError, UnicodeDecodeError) as e:
                    requeue = False
                    if isinstance(e, KleinQueueError):
                        requeue = e.requeue
                    nack_cb = functools.partial(self._consumer._negative_acknowledge_message, basic_deliver.delivery_tag, False, requeue)
                    self._consumer.threadsafe_call(nack_cb)

            except queue.Empty:
                continue

    def stop(self):
        self._closing = True


class Consumer(_Connection):
    """Consumer class listens on a message queue.

    When a message is received the handler function is called by a message worker.
    You can specify the number of workers (threads).

    `config`: The `klein_config.config.EnvironmentAwareConfig` containing connection details to rabbit.

    `key`: The `str` key in the config with specific consumer config, these are:
    ```yaml
    key:
        queue: 'queue name'         # The name of the rabbitmq queue.
        auto_acknowledge: false     # Whether to auto acknowledge messages as they are read (recommended false).
        prefetch: 10                # The number of unacknowledged messages to read from the queue at once (recommended to
                                # be equal to the number of workers).
        create_on_connect: true     # Whether to create a queue on connection.
        workers: 10                 # The number of workers (threads) that handle messages. Defaults to 1.
    ```

    ## Example
    ```python
    from klein_config.config import EnvironmentAwareConfig
    from src.klein_queue.rabbitmq.consumer import Consumer
    config = EnvironmentAwareConfig()       # Read from file specified with `--config`
    def handle_fn(message, **kwargs):       # handler_fn to be called in worker threads.
        print(message)
    consumer = Consumer(config, "consumer", handler_fn)
    consumer.run()
    ```
    """

    def __init__(self, config, key, handler_fn=None):
        self._queue = config.get(key)
        self._config = config
        self.handler_fn = handler_fn
        self._handler_thread = None
        self._consumer_tag = None
        self._message_queue = queue.Queue()
        self._workers = []

        workers = self._queue.get("workers", 1)
        LOGGER.info('Starting %d MessageWorker threads', workers)
        # spawn a number of worker threads (defaults to 1)
        for _ in range(workers):
            worker = _MessageWorker(self)
            worker.start()
            self._workers.append(worker)

        super().__init__(config, key)

    def set_handler(self, handler_fn):
        """Set a new handler function on the Consumer to be called by the worker threads on receipt of a new message."""
        self.handler_fn = handler_fn

    def start(self):
        """Creates a connection to mongo, starts receiving messages, and starts processing messages with workers."""
        super()._run()

    def run(self):
        """Creates a connection to mongo, starts receiving messages, and starts processing messages with workers."""
        super()._run()

    def stop(self):
        """Cleanly closes all worker threads, stops receiving messages, and closes the rabbitmq channel and
        connection."""
        super().stop()

    def _start_activity(self):
        LOGGER.debug('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            on_message_callback=self._on_message, queue=self._queue["queue"])

    def _negative_acknowledge_message(self, delivery_tag, multiple, requeue):
        '''
        Sends a negative acknowledgement (NACK)
        '''
        LOGGER.debug("Sending negative acknowledgement on message # %s, requeue: %s", delivery_tag, requeue)
        self._channel.basic_nack(delivery_tag, multiple, requeue)

    def _on_message(self, channel, basic_deliver, properties, body):
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
    
    def _stop_activity(self):
        if self._channel:
            LOGGER.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self._consumer_tag, self._on_cancelok)

        # stop worker threads
        for worker in self._workers:
            worker.stop()
