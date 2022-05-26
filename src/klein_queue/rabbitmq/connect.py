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

import abc
import logging
import random
import pika
import pika.exceptions

LOGGER = logging.getLogger(__name__)


class _Connection:
    """
    Base connection class for the publisher and consumer workers to inherit from.
    """

    @staticmethod
    def __get_url_parameters(conf):
        conns = []
        hosts = conf.get("rabbitmq.host")

        if isinstance(hosts, str):
            hosts = hosts.split(",")

        random.shuffle(hosts)

        authority = f'{conf.get("rabbitmq.username")}:{conf.get("rabbitmq.password")}@'
        if authority == ':@':
            authority = ''

        url = f'amqp://{authority}{hosts[0]}:{conf.get("rabbitmq.port")}/'
        connection_params = pika.URLParameters(url)

        connection_params._virtual_host = conf.get("rabbitmq.vhost", "/")
        connection_params.socket_timeout = conf.get(
            "rabbitmq.socket_timeout", 5)
        connection_params.heartbeat = conf.get(
            "rabbitmq.heartbeat", 120)
        connection_params.blocked_connection_timeout = conf.get(
            "rabbitmq.blocked_connection_timeout", 300)
        connection_params.retry_delay = conf.get(
            "rabbitmq.retry_delay", 10)
        conns.append(connection_params)

        return conns

    def __init__(self, config, key, exchange=None):
        self._connection_params = self.__get_url_parameters(config)
        self._config = config
        self._key = key
        self._connection = None
        self._channel = None
        self._closing = False
        self._exchange = exchange if exchange else config.get(f"{key}.exchange", config.get("rabbitmq.exchange", ""))
        self._exchange_type = config.get(f"{key}.exchange_type", config.get("rabbitmq.exchange_type", "direct"))
        self._bind_arguments = config.get(f"{key}.exchange_bind_arguments", {})

    def connect(self):
        """
        Create new connection to rabbitmq server.
        """

        for connection in self._connection_params:
            try:
                return pika.SelectConnection(
                    parameters=connection,
                    on_open_callback=self.on_connection_open,
                    on_open_error_callback=self.on_connection_open_error,
                    on_close_callback=self.on_connection_closed,
                )

            except pika.exceptions.ConnectionClosedByBroker:
                print('Connection closed by broker')
                continue

            except pika.exceptions.AMQPChannelError as err:
                print(f"Caught a channel error: {format(err)}, stopping...")
                break

            # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError:
                print("Connection was closed, retrying...")
                continue

        return None

    def on_connection_open(self, unused_connection):
        # pylint: disable=unused-argument
        """
        Callback passed to connection.
        Triggered on successfully opening connection.
        Opens channel.
        """

        LOGGER.debug('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        LOGGER.error('Connection open failed, reopening in 5 seconds: %s', err)
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(self, connection, reason):
        # pylint: disable=unused-argument
        """
        when connection closed intentionally stop the ioloop
        otherwise attempt to reconnect
        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._connection.ioloop.call_later(5, self.reconnect)

    def reconnect(self):
        """
        stop ioloop and if not intentional reconnect immediately
        """
        LOGGER.debug('Reconnect channel: current closing state is %s', self._closing)
        self._connection.ioloop.stop()
        if not self._closing:
            self._connection = self.connect()
            self._connection.ioloop.start()

    def open_channel(self):
        """
        open channel to rabbitmq and bind callback
        """
        LOGGER.debug('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """
        on successful open of channel then bind close callback
        configures qos
        also setup exchanges
        """
        LOGGER.debug('Channel opened')
        self._channel = channel

        connection = self._config.get(self._key)
        prefetch = 1
        if "concurrency" in connection:
            prefetch = int(connection["concurrency"])
        self._channel.basic_qos(prefetch_count=prefetch)

        self.add_on_channel_close_callback()
        self.setup_exchanges()

    def add_on_channel_close_callback(self):
        """
        bind on close channel callback
        """
        LOGGER.debug('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """
        if channel closed then log and close connection
        """
        LOGGER.info("connection to channel %s closed because %s", channel, reason)
        try:
            self._connection.close()
        except pika.exceptions.ConnectionWrongStateError:
            LOGGER.warning("connection to channel %s is already closed or is closing", channel)

    def setup_exchanges(self):
        """
        if an exchange is configured then declare
        and bind callback for successful declaration
        if no exchanges configured then setup queues
        """
        if self._exchange:
            LOGGER.debug('Declaring exchange %s', self._exchange)
            self._channel.exchange_declare(
                self._exchange,
                self._exchange_type,
                callback=self.on_exchange_declareok,
                durable=True
            )
        else:
            LOGGER.debug('No exchanges to declare so setting up queue')
            self.setup_queue()

    def on_exchange_declareok(self, unused_frame):
        # pylint: disable=unused-argument
        """
        on successful declaration of exchange then setup queue
        """
        LOGGER.debug('Exchange declared')
        connection = self._config.get(self._key)
        if connection.has("queue"):
            self.setup_queue()
        else:
            LOGGER.debug('No queue to declare - starting activity')
            self._start_activity()

    def setup_queue(self):
        """
        declare queue with rabbitmq, ensuring durability
        """
        connection = self._config.get(self._key)
        create_queue = self._config.get("rabbitmq.create_queue_on_connect", True) and not (
                "create_on_connect" in connection and not connection["create_on_connect"])
        if create_queue:
            LOGGER.debug('Declaring queue %s', connection["queue"])
            self._channel.queue_declare(queue=connection["queue"],
                                        callback=self.on_queue_declareok,
                                        durable=True,
                                        exclusive=False,
                                        auto_delete=False,
                                        arguments={
                                            "queue-mode": "lazy"
                                        })

    def on_queue_declareok(self, method_frame):
        # pylint: disable=unused-argument
        """
        if exchanges configured then bind queue to exchange
        """
        connection = self._config.get(self._key)
        if self._exchange:
                LOGGER.debug('Binding %s to %s', self._exchange, connection["queue"])
                self._channel.queue_bind(
                    connection["queue"], self._exchange, arguments=self._bind_arguments, callback=self.on_bindok)
        else:
            self._start_activity()

    def on_bindok(self, unused_frame):
        # pylint: disable=unused-argument
        """
        start consuming using abstract method from descendent
        """
        LOGGER.debug('Queue bound')
        self._start_activity()

    def acknowledge_message(self, delivery_tag):
        """
        ack message
        """
        LOGGER.debug('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def on_cancelok(self, unused_frame):
        # pylint: disable=unused-argument
        """
        if cancelled then close channel
        """
        LOGGER.debug('RabbitMQ acknowledged the cancellation of the activity')
        self.close_channel()

    def add_on_cancel_callback(self):
        """
        bind callback to cancel
        """
        LOGGER.debug('Adding activity cancellation callback')
        self._channel.add_on_cancel_callback(self.on_activity_cancelled)

    def on_activity_cancelled(self, method_frame):
        """
        close channel
        """
        LOGGER.debug(
            'Activity was cancelled remotely, shutting down: %r', method_frame)
        if self._channel:
            self.close_channel()

    def close_channel(self):
        """
        close channel
        """
        LOGGER.debug('Closing the channel')
        self._channel.close()

    def run(self):
        """
        start connection and ioloop
        """
        self._connection = self.connect()
        self._connection.ioloop.start()
        while not self._closing:
            try:
                self._connection = self.connect()
                self._connection.ioloop.start()

            except KeyboardInterrupt:
                self.stop()
                if (self._connection is not None and
                        not self._connection.is_closed):
                    # Finish closing
                    self._connection.ioloop.start()

        LOGGER.info('Stopped')

    def stop(self):
        """
        cleanly stop and close connection
        """
        LOGGER.debug('Stopping')
        self._closing = True
        self._stop_activity()
        self._connection.ioloop.stop()
        LOGGER.debug('Stopped')

    def close_connection(self):
        """
        close connection
        """
        LOGGER.debug('Closing connection')
        self._connection.close()

    def threadsafe_call(self, cb):
        """
        Runs a callback in the context of the IO loop
        :param cb: a callback function
        """
        self._connection.ioloop.add_callback_threadsafe(cb)

    @abc.abstractmethod
    def _stop_activity(self):
        """
        You must define a stop_activity method
        """
        return

    @abc.abstractmethod
    def _start_activity(self):
        """
        You must define a start_activity method
        """
        return
