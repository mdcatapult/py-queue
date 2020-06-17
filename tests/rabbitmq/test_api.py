import argparse
from unittest import mock

yamlString = """
rabbitmq:
  host: [localhost]
  port: 5672
  management_port: 15672
  username: doclib
  password: doclib

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


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


def side_effect(*args, **kwargs):
    body = [
        {
            'destination': 'archive',
            'destination_type': 'queue'
        },
        {
            'destination': 'supervisor',
            'destination_type': 'queue'
        },
        {
            'destination': 'something else',
            'destination_type': 'not a queue'
        }
    ]
    return MockResponse(body, 200)


class TestApi:
    @mock.patch('src.klein_queue.rabbitmq.api.requests.get')
    @mock.patch('argparse.ArgumentParser.parse_known_args',
                return_value=(argparse.Namespace(config="dummy.yml", common=None), argparse.Namespace()))
    @mock.patch('builtins.open', new_callable=mock.mock_open, read_data=yamlString)
    def test_list_queues(self, mock_open, mock_args, mock_req):
        mock_req.side_effect = side_effect

        import src.klein_queue.rabbitmq.api as api
        queues = api.list_queues("doclib")
        mock_req.assert_called_with('http://localhost:15672/api/exchanges/%2f/doclib/bindings/source',
                                    auth=('doclib', 'doclib'))
        assert queues == ['archive', 'supervisor']
