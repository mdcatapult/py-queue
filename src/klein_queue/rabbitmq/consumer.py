# copyright 2022 Medicines Discovery Catapult
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -*- coding: utf-8 -*-
import functools
import json
import logging
import queue
import threading
import time

from ..errors import KleinQueueError
from .connect import _Connection

LOGGER = logging.getLogger(__name__)


class _MessageWorker(threading.Thread):
    """
    Message worker class
    """

    def __init__(self, consumer, exception_handler=None):
        self._consumer = consumer
        self._closing = False
        self._exception_handler = exception_handler

        self._is_active = False
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
                (body, properties, basic_deliver) = self._consumer._message_queue.get(True, 1)
                self._is_active = True

                try:
                    self._consumer.handler_fn(json.loads(
                        body), basic_deliver=basic_deliver, properties=properties)

                    if not self._consumer.auto_ack:
                        LOGGER.info("Acknowledging successfully processed message # %s", basic_deliver.delivery_tag)
                        ack_cb = functools.partial(self._consumer.acknowledge_message, basic_deliver.delivery_tag)
                        self._consumer.threadsafe_call(ack_cb)
                except BaseException as exception:
                    def nack(requeue):
                        nack_cb = functools.partial(self._consumer._negative_acknowledge_message,
                                                    basic_deliver.delivery_tag, False, requeue)
                        self._consumer.threadsafe_call(nack_cb)
                    if self._consumer.auto_ack:
                        LOGGER.exception("Exception occurred during processing of message # %s:"
                                         " Already acknowledged", basic_deliver.delivery_tag, exc_info=exception)
                    elif isinstance(exception, KleinQueueError) and exception.requeue:  # pylint: disable=no-member
                        LOGGER.info("Exception occurred during processing of message # %s: "
                                    "Negatively acknowledging and requeuing", basic_deliver.delivery_tag)
                        nack(True)
                    elif self._exception_handler is not None:
                        self._exception_handler(exception, nack, body=body, properties=properties,
                                                basic_deliver=basic_deliver)
                    else:
                        LOGGER.exception("Exception occurred during processing of message # %s: "
                                         "Negatively acknowledging and not requeuing", basic_deliver.delivery_tag,
                                         exc_info=exception)
                        nack(False)

            except queue.Empty:
                if self._is_active:
                    self._consumer.on_empty_queue_fn()
                    self._is_active = False

    def stop(self):
        self._consumer.on_stop_fn()
        self._closing = True


class _ConsumerConnection(_Connection):
    """ConsumerConnection class listens on a message queue.

    When a message is received the handler function is called by a message worker.
    You can specify the number of workers (threads).
    """

    def __init__(self, config, key, handler_fn=None, exception_handler=None, exchange=None, on_empty_queue_fn=(lambda: None), on_stop_fn=(lambda: None)):
        self._key = key
        self._config = config
        self.handler_fn = handler_fn
        self.on_empty_queue_fn = on_empty_queue_fn
        self.on_stop_fn = on_stop_fn
        self._handler_thread = None
        self._consumer_tag = None
        self._message_queue = queue.Queue()
        self._workers = []
        self.auto_ack = self._config.get(f"{key}.auto_acknowledge", False)

        workers = int(self._config.get(f"{key}.concurrency", 1))
        LOGGER.info('Starting %d MessageWorker threads', workers)
        # spawn a number of worker threads (defaults to 1)
        for _ in range(workers):
            worker = _MessageWorker(self, exception_handler)
            worker.start()
            self._workers.append(worker)

        super().__init__(config, key, exchange=exchange)

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
            on_message_callback=self._on_message, queue=self._config.get(f"{self._key}.queue"), auto_ack=self.auto_ack)

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

        if self.auto_ack:
            LOGGER.info("Auto-acknowledged message # %s", basic_deliver.delivery_tag)

        # decode
        body = body.decode('utf-8')

        self._message_queue.put((body, properties, basic_deliver))

    def on_channel_closed(self, channel, reason):
        if not self._closing:
            LOGGER.info('Channel closed, try and reconnect in 5 seconds')
            time.sleep(5)
            self.reconnect()

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

    def __init__(self, config, key, handler_fn=None, exception_handler=None, exchange=None, on_empty_queue_fn=(lambda: None), on_stop_fn=(lambda: None)):
        """
        `config`: The `klein_config.config.EnvironmentAwareConfig` containing connection details to rabbit.

        `key`: The `str` key in the config with specific consumer config, these are:
        ```yaml
        key:                            # i.e. consumer
            queue: 'queue name'         # The name of the rabbitmq queue.
            exchange: 'exchange name'   # (Optional) the name of the exchange to consume from.
            exchange_type: 'direct'     # (Optional) the type of exchange to consume from (e.g. 'topic', 'fanout').
                                        # Defaults to 'direct'.
            exchange_bind_arguments     # (Optional) Key: value  pairs of arguments to use when binding to an exchange.
            auto_acknowledge: false     # Whether to auto acknowledge messages as they are read (recommended false).
            create_on_connect: true     # Whether to create a queue on connection.
            concurrency: 10             # The number of workers (threads) that handle messages and the number of
                                        # unacknowledged messages to read from the queue at once. Defaults to 1.
        ```
        `handler_fn`: A callback function to be executed on receipt of a new message.

        `exception_handler`: A callback function to be executed when an exception is caught during message handling.

        `on_empty_queue_fn`: An optional callback function to be executed when the queue empties

        ## Exception handling
        Exceptions raised in the handler function are handled by exactly one of the following cases. In order of
        precedence:

        1. Check if the consumer is set to auto acknowledge messages. If so, log the exception and do nothing.
        2. Check if the raised exception is of type `src.klein_queue.errors.KleinQueueError` and it's requeue attribute
        is set to `True`. If so, negatively acknowledge the message and requeue it (and log this behaviour).
        3. Check if the `exception_handler` has been set. If so, call it.
        4. Catch all. Log the exception, negatively acknowledge the message and do not requeue it.

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
            exchange: 'test_exchange' # You can also define an exchange here if it is used by multiple consumers.
        consumer:
            name: test.consumer
            queue: test
            auto_acknowledge: false
            concurrency: 2
            create_on_connect: true
            exchange: test_events_exchange
            exchange_type: headers
            exchange_bind_arguments:
                db: test_db
                coll: test_coll
                x-match: any (default in rabbitMQ is all)
        ```
        **terminal**
        ```bash
        $ python main.py --config config.yaml
        ```
        """

        self._consumer = _ConsumerConnection(config, key, handler_fn, exception_handler, exchange, on_empty_queue_fn, on_stop_fn)
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
