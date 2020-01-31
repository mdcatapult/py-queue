import argparse
import threading

import mock

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
  
error:
  queue: errors

publisher:
  queue: publish
"""


class CustomThrowable(Exception):
    pass


class TestConsumer:

    @mock.patch('argparse.ArgumentParser.parse_known_args',
                return_value=(argparse.Namespace(config="dummy.yml", common=None), argparse.Namespace()))
    @mock.patch('builtins.open', new_callable=mock.mock_open, read_data=yamlString)
    def test_consumption(self, mock_open, mock_args):

        def handler_fn(msg, **kwargs):
            assert msg == {'msg': 'test_message'}
            raise CustomThrowable

        from klein_config.config import EnvironmentAwareConfig
        config = EnvironmentAwareConfig()
        mock_open.assert_called_with('dummy.yml', 'r')

        # spin out into new thread
        def consume():
            try:
                from src.klein_queue.rabbitmq.asynchronous.consumer import Consumer
                consumer = Consumer(config.get('consumer'), handler_fn)
                consumer.run()
            except CustomThrowable:
                assert True

        def publish():
            from src.klein_queue.rabbitmq.asynchronous.publisher import Publisher
            publisher = Publisher(config.get('consumer'))
            publisher.add({'msg': 'test_message'})
            publisher.run()

        c = threading.Thread(target=consume)
        p = threading.Thread(target=publish)

        c.start()
        p.start()
