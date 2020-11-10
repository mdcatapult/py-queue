# -*- coding: utf-8 -*-
import json
import logging
import functools
import random
import string
import threading
import queue
from .connect import _Connection
from .exceptions import new_default_exception_handler

LOGGER = logging.getLogger(__name__)


class _MessageWorker(threading.Thread):
    """
    Message worker class
    """

    def __init__(self, consumer, exception_handler=None):
        self._consumer = consumer
        self._closing = False
        if exception_handler is not None:
            self._exception_handler = exception_handler
        else:
            self._exception_handler = new_default_exception_handler()
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
                (basic_deliver, properties, body, auto_ack) = self._consumer._message_queue.get(True, 1)

                try:
                    self._consumer.handler_fn(json.loads(
                        body), basic_deliver=basic_deliver, properties=properties)

                    if not auto_ack:
                        LOGGER.info("Acknowledging successfully processed message # %s", basic_deliver.delivery_tag)
                        ack_cb = functools.partial(self._consumer.acknowledge_message, basic_deliver.delivery_tag)
                        self._consumer.threadsafe_call(ack_cb)
                except BaseException as exception:
                    def nack(requeue):
                        nack_cb = functools.partial(self._consumer._negative_acknowledge_message,
                                                    basic_deliver.delivery_tag, False, requeue)
                        self._consumer.threadsafe_call(nack_cb)
                    if auto_ack:
                        LOGGER.info("Exception occurred during processing of message # %s", basic_deliver.delivery_tag)
                    else:
                        self._exception_handler(exception, nack, body=body, properties=properties,
                                                basic_deliver=basic_deliver)

            except queue.Empty:
                continue

    def stop(self):
        self._closing = True


class _ConsumerConnection(_Connection):
    """ConsumerConnection class listens on a message queue.

    When a message is received the handler function is called by a message worker.
    You can specify the number of workers (threads).
    """

    def __init__(self, config, key, handler_fn=None, exception_handler=None):
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
            worker = _MessageWorker(self, exception_handler)
            worker.start()
            self._workers.append(worker)

        super().__init__(config, key)

    def set_handler(self, handler_fn):
        """Set a new handler function on the Consumer to be called by the worker threads on receipt of a new message."""
        self.handler_fn = handler_fn

    def start(self):
        """Creates a connection to mongo, starts receiving messages, and starts processing messages with workers."""
        super().run()

    def run(self):  # pylint: disable=useless-super-delegation
        """Creates a connection to mongo, starts receiving messages, and starts processing messages with workers."""
        super().run()

    def stop(self):  # pylint: disable=useless-super-delegation
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

    def _on_message(self, channel, basic_deliver, properties, body):  # pylint: disable=unused-argument
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

        self._message_queue.put((basic_deliver, properties, body, auto_ack))

    def _stop_activity(self):
        if self._channel:
            LOGGER.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self._consumer_tag, self.on_cancelok)

        # stop worker threads
        for worker in self._workers:
            worker.stop()


class Consumer(threading.Thread):
    """Multithreaded consumer
    """

    def __init__(self, config, key, handler_fn=None, exception_handler=None):
        """
        `config`: The `klein_config.config.EnvironmentAwareConfig` containing connection details to rabbit.

        `key`: The `str` key in the config with specific consumer config, these are:
        ```yaml
        key:                            # i.e. consumer
            queue: 'queue name'         # The name of the rabbitmq queue.
            auto_acknowledge: false     # Whether to auto acknowledge messages as they are read (recommended false).
            prefetch: 10                # The number of unacknowledged messages to read from the queue at once
                                        # (recommended to be equal to the number of workers).
            create_on_connect: true     # Whether to create a queue on connection.
            workers: 10                 # The number of workers (threads) that handle messages. Defaults to 1.
        ```
        `handler_fn`: A callback function to be executed on receipt of a new message.

        `exception_handler`: A callback function to be executed when an exception is caught during message handling.
        This defaults to `src.klein_queue.exceptions.new_default_exception_handler`.

        ## Example
        **main.py**
        ```python
        from klein_config.config import EnvironmentAwareConfig
        from klein_queue.rabbitmq.consumer import Consumer

        config = EnvironmentAwareConfig()       # Read from file specified with `--config`
        def handle_fn(message, **kwargs):       # handler_fn to be called in worker threads.
            print(message)
        consumer = Consumer(config, "consumer", handler_fn)
        consumer.start()

        ```
        **config.yaml**
        ```python
        rabbitmq:
            host: [localhost]
            port: 5672
            username: guest
            password: guest
            heartbeat: 2
        consumer:
            name: test.consumer
            queue: test
            auto_acknowledge: false
            prefetch: 2
            create_on_connect: true
            error: error
            workers: 2
        ```
        **terminal**
        ```bash
        $ python main.py --config config.yaml
        ```
        """

        self._consumer = _ConsumerConnection(config, key, handler_fn, exception_handler)
        super().__init__()

    def set_handler(self, handler_fn):
        """Set a new handler function on the Consumer to be called by the worker threads on receipt of a new message."""
        self._consumer.set_handler(handler_fn)

    def start(self):  # pylint: disable=useless-super-delegation
        """Creates a connection to rabbit ***in a new thread***, starts receiving messages, and starts processing
        messages with workers."""
        super().start()

    def run(self):
        """Creates a connection to rabbit ***in the current thread***, starts receiving messages, and starts processing
        messages with workers. This will block the current thread."""
        self._consumer.run()

    def stop(self):
        """Cleanly closes all worker threads, stops receiving messages, and closes the rabbitmq channel and
        connection."""
        self._consumer.threadsafe_call(self._consumer.stop)
