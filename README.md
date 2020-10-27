# Klein Queue

Module to abstract queues. Currently only implements RabbitMQ, roadmap to include alternatives such as Kafka.

## Documentation

API docs can be found at https://informatics.pages.mdcatapult.io/klein/py-queue/src.

To view API docs for a particular version can be generated with `pdoc` with:
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


## Python

Utilises python 3.7

### Ubuntu

```
sudo apt install python3.7
```

## Virtualenv

```
virtualenv -p python3.7 venv
source venv/bin/activate
echo -e "[global]\nindex = https://nexus.mdcatapult.io/repository/pypi-all/pypi\nindex-url = https://nexus.mdcatapult.io/repository/pypi-all/simple" > venv/pip.conf
pip install -r requirements.txt
```

### Testing
```bash
docker-compose up
python -m pytest
```