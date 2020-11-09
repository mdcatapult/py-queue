import threading
from random import randint
import time
from src.klein_queue.errors import KleinQueueError


class TestConsumer:

    def test_consumption(self):
        event = threading.Event()

        def handle_handle(cons):
            def handler_fn(msg, **kwargs):
                assert msg == {'msg': 'test_message'}
                event.set()
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
                "queue": "pytest.consume",
                "auto_acknowledge": True,
                "prefetch": 1,
                "create_on_connect": True,
            },
            "publisher": {
                "queue": "pytest.consume"
            }
        })
      
        from src.klein_queue.rabbitmq.consumer import Consumer
        consumer = Consumer(config, "consumer")
        consumer.set_handler(handle_handle(consumer))

        c = threading.Thread(target=consumer.run)
        c.start()

        from src.klein_queue.rabbitmq.publisher import Publisher
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

        from klein_config.config import EnvironmentAwareConfig
        config = EnvironmentAwareConfig({
            "rabbitmq": {
                "host": ["localhost"],
                "port": 5672,
                "username": "doclib",
                "password": "doclib",
            },
            "consumer": {
                "queue": "pytest.concurrency",
                "auto_acknowledge": True,
                "prefetch": workers,
                "create_on_connect": True,
                "workers": workers,
            },
            "publisher": {
                "queue": "pytest.concurrency"
            }
        })

        from src.klein_queue.rabbitmq.consumer import Consumer
        consumer = Consumer(config, "consumer", handler_fn)

        # check number of threads spawned
        assert len(consumer._consumer._workers) == workers

        c = threading.Thread(target=consumer.run)
        c.start()

        from src.klein_queue.rabbitmq.publisher import Publisher
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

    def test_error_publishing_exception_handler(self):
        def handler_fn(msg, **kwargs):
            raise KleinQueueError("forced error")

        def error_handler_fn(msg, properties=None, **kwargs):
            nonlocal waiting
            waiting = False

        from klein_config.config import EnvironmentAwareConfig
        config = EnvironmentAwareConfig({
            "rabbitmq": {
                "host": ["localhost"],
                "port": 5672,
                "username": "doclib",
                "password": "doclib",
            },
            "consumer": {
                "queue": "pytest.concurrency",
            },
            "publisher": {
                "queue": "pytest.concurrency"
            },
            "error_publisher": {
                "queue": "errors"
            },
            "error_consumer": {
                "queue": "errors"
            }
        })

        from src.klein_queue.rabbitmq.publisher import Publisher
        error_publisher = Publisher(config, "error_publisher")
        error_publisher.start()
        upstream_publisher = Publisher(config, "consumer")
        upstream_publisher.start()

        from src.klein_queue.rabbitmq.exceptions import new_error_publishing_exception_handler
        exception_handler = new_error_publishing_exception_handler("consumer", upstream_publisher, error_publisher)

        from src.klein_queue.rabbitmq.consumer import Consumer
        consumer = Consumer(config, "consumer", handler_fn, exception_handler=exception_handler)
        consumer.start()

        waiting = True
        error_consumer = Consumer(config, "error_consumer", error_handler_fn)
        error_consumer.start()

        publisher = Publisher(config, "publisher")
        publisher.start()
        publisher.publish("message")

        while waiting:
            pass

        publisher.stop()
        upstream_publisher.stop()
        error_publisher.stop()
        consumer.stop()
        error_consumer.stop()
