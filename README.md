# Klein Queue

Module to abstract queues. Currently implements RabbitMQ.

## Documentation

Generate API docs for a particular version with `pdoc`:
```bash
pip install pdoc3
pdoc --http :8080 src
```

## Environment Variables


| Env Variable                        | Description                                                    |
|-------------------------------------|-------------                                                |
| RABBITMQ_USERNAME                   |                                                             |
| RABBITMQ_PASSWORD                   |                                                             |
| RABBITMQ_HOST                       |                                                             |
| RABBITMQ_PORT                       |                                                             |
| RABBITMQ_VHOST                      | Use a VHOST instead of default of /                         |
| RABBITMQ_SOCKET_TIMEOUT             |                                                             |
| RABBITMQ_HEARTBEAT                  |                                                             |
| RABBITMQ_BLOCKED_CONNECTION_TIMEOUT |                                                             |
| RABBITMQ_RETRY_DELAY                |                                                             |
| RABBITMQ_PUBLISHER                  |                                                             |
| RABBITMQ_CONSUMER                   |                                                             |
| RABBITMQ_ERROR                      |                                                             |
| RABBITMQ_CREATE_QUEUE_ON_CONNECT    |Config to determine whether to create queue at connection    |

## Example usage

### Consumer

Define the following in a `config.yaml` file.

```yaml
rabbitmq:
  host: [localhost]
  port: 5672
  username: guest
  password: guest
  heartbeat: 2
  exchange: 'test_exchange' # You can also define an exchange here if it is used by multiple consumers.
consumer:
  name: test.consumer
  queue: test
  auto_acknowledge: false
  concurrency: 2
  create_on_connect: true
  exchange: test_events_exchange
  exchange_type: headers
  exchange_bind_arguments:
  db: test_db
  coll: test_coll
  x-match: any # default in rabbitMQ is all
publisher:
  queue: test
  create_on_connect: true
```

Add the following to your `main.py` file.

```python
from klein_config.config import EnvironmentAwareConfig
from klein_queue.rabbitmq.consumer import Consumer

config = EnvironmentAwareConfig()       # Read from file specified with `--config`
def handler_fn(message, **kwargs):      # handler_fn receives messages from the queue.
    print(message)
consumer = Consumer(config, "consumer", handler_fn)
consumer.start()
```

Run the following command to start the consumer.

```bash
$ python main.py --config config.yaml
```
### Publisher

Using the same config as the consumer, you can create a publisher.
Add the following to a python file and run it. It will publish a message to the queue. If you
have the consumer running it will print the message to the console.

```python
from klein_config.config import EnvironmentAwareConfig
from klein_queue.rabbitmq.publisher import Publisher

config = EnvironmentAwareConfig()       # Read from file specified with `--config`

publisher = Publisher(config, "publisher")
if __name__ == "__main__":
    publisher.start()                   # spawns the publisher thread
    publisher.add({'id': 'abc123'})     # sends a message
```

See the `tests` directory for more examples.

## Python

Utilises python 3.11

## Virtualenv

```
virtualenv -p python3.11 venv
source venv/bin/activate
pip install -r requirements.txt
```

### Testing
```bash
docker-compose up
python -m pytest
```
## License
This project is licensed under the terms of the Apache 2 license, which can be found in the repository as `LICENSE.txt`
