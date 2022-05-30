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
import json
import logging
from threading import Thread
from collections import deque
import pika
from .connect import _Connection

LOGGER = logging.getLogger(__name__)


class Publisher(Thread):
    """Multithreaded publisher.

    We use a multithreaded publisher to keep the I/O loop (and heartbeat) alive and maintain a persistent connection.
    This is more efficient than creating a new connection for every message.
    """

    def __init__(self, config, key, exchange=None):
        """
        `config`: The `klein_config.config.EnvironmentAwareConfig` containing connection details to rabbit.

        `key`: The `str` key in the config with specific publisher config, these are:
        ```yaml
        key:                            # i.e. upstream
            queue: 'queue name'         # The name of the rabbitmq queue.
            create_on_connect: true     # Whether to create a queue on connection.
            exchange: 'exchange name'   # (Optional) the name of the exchange to publish to (defaults to the default
                                        # exchange).
            exchange_type: 'direct'     # (Optional) the type of exchange to consume from (e.g. 'topic', 'fanout').
                                        # Defaults to 'direct'.
            confirm_delivery: false     # (Optional) toggles delivery confirmations. Defaults to true.
        ```
        ## Example
        **main.py**
        ```python
        from klein_config.config import EnvironmentAwareConfig
        from klein_queue.rabbitmq.publisher import Publisher

        config = EnvironmentAwareConfig()       # Read from file specified with `--config`

        publisher = Publisher(config, "publisher")
        if __name__ == "__main__":
            publisher.start()                   # spawns the publisher thread
            publisher.add({'id': 'abc123'})     # sends a message
        
        ```
        **config.yaml**
        ```yaml
        rabbitmq:
            host: [localhost]
            port: 5672
            username: guest
            password: guest
            heartbeat: 2
            exchange: 'test_exchange' # You can also define an exchange here if it is used by multiple consumers.
        publisher:
            queue: test
            create_on_connect: true
        ```
        **terminal**
        ```bash
        $ python main.py --config config.yaml
        ```
        """
        self._publisher = _PublisherWorker(config, key, exchange=exchange)
        self.queue = config.get(f"{format(key)}.queue", '')
        super().__init__()

    def run(self):
        """
        Start the publisher & run it's IO loop ***within the current thread***. This will block the current thread and
        is *not recommended*.
        """
        self._publisher.run()

    def add(self, message, properties=None, persist=True):
        """
        Adds a `message` (`dict`) to the internal queue to be published with the set `properties`.
        If you do not wish to persist your messages, you must explicitly set `persist` to `False`.
        """
        if persist and properties is None:
            properties = pika.BasicProperties(delivery_mode=2)
        elif persist:
            properties.delivery_mode = 2
        self._publisher.publish(message, properties)

    def publish(self, message, properties=None, persist=True):
        """
        Adds a `message` to the internal queue - alias of `src.klein_queue.rabbitmq.publisher.Publisher.add`.
        """
        self.add(message, properties, persist)

    def stop(self):
        """
        Stops the publisher and closes the connection to rabbit.
        """
        self._publisher.threadsafe_call(self._publisher.stop)

    def start(self):  # pylint: disable=useless-super-delegation
        """
         Start the publisher & run it's IO loop ***in a seperate thread***.
        """
        super().start()


class _PublisherWorker(_Connection):

    def __init__(self, config, key, exchange=None):
        self._messages = deque([])
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._stopping = False
        self._key = key
        d = config.get(f"{key}.confirm_delivery", "true")
        self._confirm_delivery = d is True or (isinstance(d, str) and d.lower() in ["true", "1", "yes"])

        super().__init__(config, key, exchange=exchange)

    def _start_activity(self):
        LOGGER.debug('Issuing consumer related RPC commands')
        if self._confirm_delivery:
            self.enable_delivery_confirmations()
        self.schedule_next_message()

    def _stop_activity(self):
        self._stopping = True
        self.close_channel()
        self.close_connection()

    def enable_delivery_confirmations(self):
        LOGGER.debug('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOGGER.debug('Received %s for delivery tag: %i',
                     confirmation_type,
                     method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        LOGGER.debug('Published %i messages, %i have yet to be confirmed, '
                     '%i were acked and %i were nacked',
                     self._message_number, len(self._deliveries),
                     self._acked, self._nacked)

    def schedule_next_message(self):
        if self._stopping:
            return

        LOGGER.debug('Scheduling next message')
        self._connection.ioloop.add_callback_threadsafe(self.__publish_message)

    def __publish_message(self):
        if self._stopping:
            LOGGER.debug(
                'Publisher currently stopping, unable to publish messages at this time')
            return

        if not self._messages:
            # no messages to publish... do nothing
            self.schedule_next_message()
            return

        (message, properties) = self._messages.popleft()

        connection = self._config.get(self._key)

        if connection.has("queue"):
            LOGGER.debug('Publishing message to queue %s', connection.get("queue"))
        elif connection.has("exchange"):
            LOGGER.debug('Publishing message to exchange %s', connection.get("exchange"))
        self._channel.basic_publish(self._exchange,
                                    connection.get("queue", ''),
                                    json.dumps(message),
                                    properties)

        self._message_number += 1
        if self._confirm_delivery:
            self._deliveries.append(self._message_number)
        LOGGER.debug('Published message # %i', self._message_number)
        self.schedule_next_message()

    def publish(self, message, properties=None):
        LOGGER.debug(
            'Adding message to internal stack ready for publishing')
        self._messages.append((message, properties))
