import threading
from random import randint
import pika
import time
from src.klein_queue.errors import KleinQueueError
from src.klein_queue.rabbitmq.publisher import Publisher
from src.klein_queue.rabbitmq.consumer import Consumer
from klein_config.config import EnvironmentAwareConfig

test_config = {
    "rabbitmq": {
        "host": ["localhost"],
        "port": 5672,
        "username": "doclib",
        "password": "doclib",
    },
    "consumer": {
        "auto_acknowledge": True,
        "prefetch": 1,
        "workers": 1,
        "create_on_connect": True,
    },
    "publisher": {}
}


class TestConsumer:

    def test_consumption(self):
        event = threading.Event()

        def handle_handle(cons):
            def handler_fn(msg, **kwargs):
                assert msg == {'msg': 'test_message'}
                event.set()
                cons.stop()

            return handler_fn

        config = EnvironmentAwareConfig({
            **test_config,
            "consumer": {
                "queue": "pytest.consume",
                "auto_acknowledge": True,
                "create_on_connect": True
            },
            "publisher": {
                "queue": "pytest.consume"
            }
        })

        consumer = Consumer(config, "consumer")
        consumer.set_handler(handle_handle(consumer))

        c = threading.Thread(target=consumer.run)
        c.start()

        publisher = Publisher(config, "publisher")
        publisher.start()
        publisher.publish({'msg': 'test_message'})

        # timeout = 10 seconds on waiting for message to arrive
        message_received_in_time = event.wait(10)
        assert message_received_in_time

        consumer.stop()
        publisher.stop()

    def test_worker_concurrency(self):
        workers = randint(2, 5)
        events = []

        def handler_fn(msg, **kwargs):
            event_id = msg['event']
            events[event_id].set()
            time.sleep(10)  # sleep to block this worker

        config = EnvironmentAwareConfig({
            **test_config,
            "consumer": {
                "queue": "pytest.concurrency",
                "prefetch": workers,
                "workers": workers,
            },
            "publisher": {
                "queue": "pytest.concurrency"
            }
        })

        consumer = Consumer(config, "consumer", handler_fn)

        # check number of threads spawned
        assert len(consumer._consumer._workers) == workers

        c = threading.Thread(target=consumer.run)
        c.start()

        publisher = Publisher(config, "publisher")
        publisher.start()

        for i in range(workers):
            # send one message for each worker
            events.append(threading.Event())
            publisher.publish({'event': i})

        for i in range(workers):
            message_received_in_time = events[i].wait(5)
            assert message_received_in_time

        consumer.stop()
        publisher.stop()

    def test_default_exception_handler(self):
        retries = 0
        waiting = True

        def handler_fn(msg, **kwargs):
            nonlocal waiting, retries
            retries += 1
            if retries >= 10:
                waiting = False
                raise KleinQueueError("forced error")
            else:
                raise KleinQueueError("forced error", requeue=True)

        config = EnvironmentAwareConfig({
            **test_config,
            "consumer": {
                "queue": "pytest.default_exceptions",
                "auto_acknowledge": False,
            },
            "publisher": {
                "queue": "pytest.default_exceptions"
            }
        })

        consumer = Consumer(config, "consumer", handler_fn)
        consumer.start()

        publisher = Publisher(config, "publisher")
        publisher.start()
        publisher.publish("message")

        while waiting:
            pass

        consumer.stop()
        publisher.stop()

    def test_error_publishing_exception_handler(self):
        test_message = {"id": "d5d581bb-8b42-4d1e-bbf9-3fee91ab5920"}
        error_message = ""
        error_properties = pika.BasicProperties()

        def handler_fn(msg, **kwargs):
            raise KleinQueueError("forced error")

        def error_handler_fn(msg, properties=None, **kwargs):
            nonlocal waiting, error_message, error_properties
            error_message = msg
            error_properties = properties
            waiting = False

        config = EnvironmentAwareConfig({
            **test_config,
            "consumer": {
                "queue": "pytest.exceptions",
                "auto_acknowledge": False,
            },
            "publisher": {
                "queue": "pytest.exceptions"
            },
            "error_publisher": {
                "queue": "errors"
            },
            "error_consumer": {
                "queue": "errors",
                "auto_acknowledge": True
            }
        })

        error_publisher = Publisher(config, "error_publisher")
        error_publisher.start()
        upstream_publisher = Publisher(config, "consumer")
        upstream_publisher.start()

        from src.klein_queue.rabbitmq.exceptions import new_error_publishing_exception_handler
        exception_handler = new_error_publishing_exception_handler("consumer", upstream_publisher, error_publisher)

        consumer = Consumer(config, "consumer", handler_fn, exception_handler=exception_handler)
        consumer.start()

        waiting = True
        error_consumer = Consumer(config, "error_consumer", error_handler_fn)
        error_consumer.start()

        test_publisher = Publisher(config, "publisher")
        test_publisher.start()
        test_publisher.publish(test_message)

        while waiting:
            pass

        assert test_message == error_message
        assert error_properties.headers['x-consumer'] == "consumer"
        assert "KleinQueueError" in error_properties.headers['x-exception']
        assert error_properties.headers['x-message'] == "forced error"
        assert error_properties.headers['x-queue'] == 'pytest.exceptions'
        assert "forced error" in error_properties.headers['x-stack-trace']

        test_publisher.stop()
        upstream_publisher.stop()
        error_publisher.stop()
        consumer.stop()
        error_consumer.stop()
