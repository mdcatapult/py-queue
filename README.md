# Klein Queue

Module to abstract queues. Currently only implements RabbitMQ, roadmap to include alternatives such as Kafka.

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
python -m venv ./venv
source venv/bin/activate
echo -e "[global]\nindex = https://nexus.mdcatapult.io/repository/pypi-all/pypi\nindex-url = https://nexus.mdcatapult.io/repository/pypi-all/simple" > venv/pip.conf
pip install -r requirements.txt
```

### Testing
```bash
docker-compose up
python -m pytest
```