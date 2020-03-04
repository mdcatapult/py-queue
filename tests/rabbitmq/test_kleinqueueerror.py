import argparse

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


class TempThrowable(Exception):
    pass


class TestKleinQueueError:

    @mock.patch('argparse.ArgumentParser.parse_known_args',
                return_value=(argparse.Namespace(config="dummy.yml", common=None), argparse.Namespace()))
    @mock.patch('builtins.open', new_callable=mock.mock_open, read_data=yamlString)
    def test_raise_exception(self, mock_open, mock_args):
        
        from src.klein_queue.rabbitmq.util import KleinQueueError
        with pytest.raises(KleinQueueError) as exc_info:
            try: 
                try: 
                    raise TempThrowable("bad mojo")
                except TempThrowable as err:
                    raise KleinQueueError from err
            except KleinQueueError as ker:
                assert(isinstance(ker.__cause__, TempThrowable))
                assert(str(ker.__cause__) == "bad mojo")
                raise ker
