import argparse
import threading
from unittest import mock
import time

class CustomThrowable(Exception):
    pass


class TestConsumer:

    def test_consumption(self):
        self._event = threading.Event()

        def handle_handle(cons):
            def handler_fn(msg, **kwargs):
                assert msg == {'msg': 'test_message'}
                self._event.set()
                cons.stop()

            return handler_fn

        from klein_config.config import EnvironmentAwareConfig
        config = EnvironmentAwareConfig({
            "rabbitmq": {
                "host": ["localhost"],
                "port": 5672,
                "username": "doclib",
                "password": "doclib",
            },
            "consumer": {
                "queue": "pytest",
                "auto_acknowledge": True,
                "prefetch": 1,
                "create_on_connect": True,
            },
            "publisher": {
                "queue": "pytest"
            }
        })

        from src.klein_queue.rabbitmq.asynchronous.consumer import Consumer
        consumer = Consumer(config, "consumer")
        consumer.set_handler(handle_handle(consumer))

        c = threading.Thread(target=consumer.run)
        c.start()

        from src.klein_queue.rabbitmq.synchronous.publisher import Publisher
        publisher = Publisher(config, "publisher")
        publisher.connect()
        publisher.publish_message({'msg': 'test_message'})

        # timeout = 10 seconds on waiting for message to arrive
        message_received_in_time = self._event.wait(10)
        assert message_received_in_time

        consumer.stop()

