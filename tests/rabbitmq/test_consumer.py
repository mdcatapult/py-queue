import threading
from random import randint
import time


class CustomThrowable(Exception):
    pass


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

        c = threading.Thread(target=consumer._run)
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
        assert len(consumer._workers) == workers

        c = threading.Thread(target=consumer._run)
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
