from unittest import mock


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
    def test_list_queues(self, mock_req):
        mock_req.side_effect = side_effect

        from klein_config.config import EnvironmentAwareConfig
        config = EnvironmentAwareConfig({
            "rabbitmq": {
                "host": ["localhost"],
                "port": 5672,
                "username": "doclib",
                "password": "doclib",
                "management_port": 15672
            },
            "consumer": {
                "queue": "klein.prefetch",
                "auto_acknowledge": True,
                "prefetch": 1,
                "create_on_connect": True,
            },
            "publisher": {
                "queue": "publish"
            }
        })

        host = config.get('rabbitmq.host')
        if isinstance(host, list):
            host = host[0]
        url = 'http://%s:15672/api/exchanges/%%2f/doclib/bindings/source' % host

        from src.klein_queue.rabbitmq.api import ApiClient
        client = ApiClient(config)
        queues = client.list_queues("doclib")
        mock_req.assert_called_with(url, auth=('doclib', 'doclib'))
        assert queues == ['archive', 'supervisor']
