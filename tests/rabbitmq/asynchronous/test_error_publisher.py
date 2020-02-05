import argparse
import sys
import threading

import mock
import pytest

yamlString = """
rabbitmq:
  host: [localhost]
  port: 5672
  username: guest
  password: guest

consumer:
  queue: klein.prefetch
  auto_acknowledge: true
  prefetch: 1
  create_on_connect: true
  error: error
  
publisher:
  queue: publish

error:
  queue: error
"""


class CustomThrowable(Exception):
    pass


class TestErrorPublisher:

    @mock.patch('argparse.ArgumentParser.parse_known_args',
                return_value=(argparse.Namespace(config="dummy.yml", common=None), argparse.Namespace()))
    @mock.patch('builtins.open', new_callable=mock.mock_open, read_data=yamlString)
    def test_consumption(self, mock_open, mock_args):
        def handle_handle(cons):
            def handler_fn(msg, **kwargs):
                cons.stop_activity()
                sys.exit(-1)

            return handler_fn

        from klein_config.config import EnvironmentAwareConfig
        config = EnvironmentAwareConfig()
        mock_open.assert_called_with('dummy.yml', 'r')

        from src.klein_queue.rabbitmq.asynchronous.consumer import Consumer
        from src.klein_queue.rabbitmq.util import KleinQueueError
        consumer = Consumer(config.get('consumer'))
        consumer.set_handler(handle_handle(consumer))

        # spin out into new thread
        def consume():
            consumer.run()

        c = threading.Thread(target=consume)
        c.start()
        c.join(5.0)

        from src.klein_queue.rabbitmq.publisher import error
        with pytest.raises(KleinQueueError) as exc_info:
            error('oh dear')
            assert True is True

        assert exc_info.typename == "KleinQueueError"
