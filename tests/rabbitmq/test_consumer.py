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
    }
}


class TestConsumer:

    def test_consumption(self):
        event = threading.Event()

        def handle_handle(cons):
            def handler_fn(msg, properties=None, **kwargs):
                assert msg == {'msg': 'test_message'}
                assert properties.delivery_mode == 2
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

    def test_exchange_creation(self):
        test_message = {"id": "d5d581bb-8b42-4d1e-bbf9-3fee91ab5920"}
        delivery = pika.spec.Basic.Deliver()

        def handler_fn(msg, basic_deliver=None, **kwargs):
            nonlocal delivery, waiting
            delivery = basic_deliver
            waiting = False

        config = EnvironmentAwareConfig({
            **test_config,
            "consumer": {
                "queue": "pytest.test-queue",
                "auto_acknowledge": False,
                "concurrency": 3,
                "exchange": "test-exchange"
            },
            "publisher": {
                "queue": "pytest.test-queue",
                "exchange": "test-exchange"
            },
        })

        consumer = Consumer(config, "consumer", handler_fn)
        consumer.start()

        test_publisher = Publisher(config, "publisher")
        test_publisher.start()
        test_publisher.publish(test_message)

        waiting = True
        while waiting:
            pass

        assert delivery.exchange == "test-exchange"
        assert delivery.routing_key == "pytest.test-queue"

        test_publisher.stop()
        consumer.stop()

    def test_worker_concurrency(self):
        workers = randint(2, 5)
        events = []

        def handler_fn(msg, **kwargs):
            event_id = msg['event']
            events[event_id].set()

        config = EnvironmentAwareConfig({
            **test_config,
            "consumer": {
                "queue": "pytest.concurrency",
                "concurrency": workers,
                "auto_acknowledge": True
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
        expected_retries = 10

        def handler_fn(msg, **kwargs):
            nonlocal waiting, retries
            retries += 1
            if retries >= expected_retries:
                # Stop waiting and don't requeue
                waiting = False
                raise KleinQueueError("forced error")
            else:
                # Requeue the message
                raise KleinQueueError("forced error", requeue=True)

        config = EnvironmentAwareConfig({
            **test_config,
            "consumer": {
                "queue": "pytest.default_exceptions",
                "auto_acknowledge": False,
                "concurrency": 3,
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

        timeout = time.time() + 60
        while waiting:
            if time.time() > timeout:
                # Fails this test if the expected number of retries has not been reached within the time limit.
                assert False
            time.sleep(1)
            pass

        consumer.stop()
        publisher.stop()

    def test_error_publishing_exception_handler(self):
        test_message = {"id": "d5d581bb-8b42-4d1e-bbf9-3fee91ab5920"}
        error_message = ""
        error_properties = pika.BasicProperties()
        message_properties = pika.BasicProperties()

        def handler_fn(msg, properties=None, **kwargs):
            nonlocal message_properties
            message_properties = properties
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
                "concurrency": 3,
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

        error_consumer = Consumer(config, "error_consumer", error_handler_fn)
        error_consumer.start()

        test_publisher = Publisher(config, "publisher")
        test_publisher.start()
        test_publisher.publish(test_message)

        waiting = True
        while waiting:
            pass

        test_publisher.stop()
        upstream_publisher.stop()
        error_publisher.stop()
        consumer.stop()
        error_consumer.stop()

        assert message_properties.delivery_mode == 2
        assert message_properties.headers['x-retry'] == 3
        assert test_message == error_message
        assert error_properties.delivery_mode == 2
        assert error_properties.headers['x-consumer'] == "consumer"
        assert "KleinQueueError" in error_properties.headers['x-exception']
        assert error_properties.headers['x-message'] == "forced error"
        assert error_properties.headers['x-queue'] == 'pytest.exceptions'
        assert "forced error" in error_properties.headers['x-stack-trace']
        assert error_properties.headers["x-original-routing-key"] == "pytest.exceptions"
        assert error_properties.headers["x-original-exchange"] == ""

    def test_on_empty_queue_callback_should_run_once_single_msg(self):
        event = threading.Event()

        def handle_handle(cons):
            def handler_fn(msg, properties=None, **kwargs):
                pass
            return handler_fn

        def on_empty_queue_fn(tracker=[]):  # make use of shared instance of list
            event.set()
            tracker.append(1)
            assert len(tracker) == 1  # Run the first time only

        config = EnvironmentAwareConfig({
            **test_config,
            "consumer": {
                "queue": "pytest.on_empty_queue_callback_should_run_once_single_msg",
                "auto_acknowledge": True,
                "create_on_connect": True
            },
            "publisher": {
                "queue": "pytest.on_empty_queue_callback_should_run_once_single_msg"
            }
        })

        consumer = Consumer(config, "consumer", on_empty_queue_fn=on_empty_queue_fn)
        consumer.set_handler(handle_handle(consumer))

        c = threading.Thread(target=consumer.run)
        c.start()

        publisher = Publisher(config, "publisher")
        publisher.start()
        publisher.publish({'msg': 'test_message'})

        # on waiting for message to arrive and then hit empty queue
        message_received_in_time = event.wait(10)
        assert message_received_in_time

        consumer.stop()
        publisher.stop()

    def test_on_empty_queue_callback_should_be_called_once_multiple_msg(self):
        event = threading.Event()

        def handle_handle(cons):
            def handler_fn(msg, properties=None, **kwargs):
                pass
            return handler_fn

        def on_empty_queue_fn(tracker=[]):
            event.set()
            tracker.append(1)
            assert len(tracker) == 1  # Run once only

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

        consumer = Consumer(config, "consumer", on_empty_queue_fn=on_empty_queue_fn)
        consumer.set_handler(handle_handle(consumer))

        publisher = Publisher(config, "publisher")
        publisher.start()
        publisher.publish({'msg': 'test_message'})
        publisher.publish({'msg': 'test_message'})
        publisher.publish({'msg': 'test_message'})

        c = threading.Thread(target=consumer.run)
        c.start()

        # waiting for message to arrive and then hit empty queue
        message_received_in_time = event.wait(30)
        assert message_received_in_time

        consumer.stop()
        publisher.stop()

    def test_on_empty_queue_callback_should_not_be_called(self):
        event = threading.Event()

        def handle_handle(cons):
            def handler_fn(msg, properties=None, **kwargs):
                pass
            return handler_fn

        def on_empty_queue_fn():
            event.set()

        config = EnvironmentAwareConfig({
            **test_config,
            "consumer": {
                "queue": "pytest.on_empty_not_called",
                "auto_acknowledge": True,
                "create_on_connect": True
            },
            "publisher": {
                "queue": "pytest.on_empty_not_called"
            }
        })

        consumer = Consumer(config, "consumer", on_empty_queue_fn=on_empty_queue_fn)
        consumer.set_handler(handle_handle(consumer))

        c = threading.Thread(target=consumer.run)
        c.start()
        # timeout = 60 seconds. event should not be reached as no message is sent
        message_received_in_time = event.wait(10)
        assert not message_received_in_time

        consumer.stop()

    def test_on_stop_callback_should_be_called_after_closed_no_msg(self):
        event = threading.Event()

        def handle_handle(cons):
            def handler_fn(msg, properties=None, **kwargs):
                pass
            return handler_fn

        def on_stop_fn():
            event.set()

        config = EnvironmentAwareConfig({
            **test_config,
            "consumer": {
                "queue": "pytest.on_stop_callback_should_be_called_after_closed_no_msg",
                "auto_acknowledge": True,
                "create_on_connect": True
            },
            "publisher": {
                "queue": "pytest.on_stop_callback_should_be_called_after_closed_no_msg"
            }
        })

        consumer = Consumer(config, "consumer", on_stop_fn=on_stop_fn)
        consumer.set_handler(handle_handle(consumer))

        c = threading.Thread(target=consumer.run)
        c.start()

        time.sleep(1)  # Give the thread time to do its thing

        consumer.stop()

        # timeout = 60 seconds.
        message_received_in_time = event.wait(10)
        assert message_received_in_time

    def test_on_stop_callback_should_not_be_called_before_closed_no_msg(self):
        event = threading.Event()

        def handle_handle(cons):
            def handler_fn(msg, properties=None, **kwargs):
                pass
            return handler_fn

        def on_stop_fn():
            event.set()

        config = EnvironmentAwareConfig({
            **test_config,
            "consumer": {
                "queue": "pytest.on_stop_callback_should_not_be_called_before_closed_no_msg",
                "auto_acknowledge": True,
                "create_on_connect": True
            },
            "publisher": {
                "queue": "pytest.on_stop_callback_should_not_be_called_before_closed_no_msg"
            }
        })

        consumer = Consumer(config, "consumer", on_stop_fn=on_stop_fn)
        consumer.set_handler(handle_handle(consumer))

        c = threading.Thread(target=consumer.run)
        c.start()

        # timeout = 60 seconds.
        message_received_in_time = event.wait(10)
        assert not message_received_in_time

        consumer.stop()

    def test_on_stop_callback_should_be_called_after_closed_with_msg(self):
        event = threading.Event()

        def handle_handle(cons):
            def handler_fn(msg, properties=None, **kwargs):
                pass
            return handler_fn

        def on_stop_fn():
            event.set()

        config = EnvironmentAwareConfig({
            **test_config,
            "consumer": {
                "queue": "pytest.on_stop_callback_should_be_called_after_closed_with_msg",
                "auto_acknowledge": True,
                "create_on_connect": True
            },
            "publisher": {
                "queue": "pytest.on_stop_callback_should_be_called_after_closed_with_msg"
            }
        })

        consumer = Consumer(config, "consumer", on_stop_fn=on_stop_fn)
        consumer.set_handler(handle_handle(consumer))

        c = threading.Thread(target=consumer.run)
        c.start()

        publisher = Publisher(config, "publisher")
        publisher.start()

        publisher.publish({'msg': 'test_message'})
        time.sleep(1)  # Give the thread time to do its thing

        publisher.stop()
        consumer.stop()

        # timeout = 60 seconds.
        message_received_in_time = event.wait(10)
        assert message_received_in_time

    def test_on_stop_callback_should_not_be_called_before_closed_with_msg(self):
        event = threading.Event()

        def handle_handle(cons):
            def handler_fn(msg, properties=None, **kwargs):
                pass
            return handler_fn

        def on_stop_fn():
            event.set()

        config = EnvironmentAwareConfig({
            **test_config,
            "consumer": {
                "queue": "pytest.on_stop_callback_should_not_be_called_before_closed_with_msg",
                "auto_acknowledge": True,
                "create_on_connect": True
            },
            "publisher": {
                "queue": "pytest.on_stop_callback_should_not_be_called_before_closed_with_msg"
            }
        })

        consumer = Consumer(config, "consumer", on_stop_fn=on_stop_fn)
        consumer.set_handler(handle_handle(consumer))

        c = threading.Thread(target=consumer.run)
        c.start()

        publisher = Publisher(config, "publisher")
        publisher.start()
        publisher.publish({'msg': 'test_message'})

        # timeout = 60 seconds.
        message_received_in_time = event.wait(10)
        assert not message_received_in_time

        publisher.stop()
        consumer.stop()
