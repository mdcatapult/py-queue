# Klein Queue

Module to abstract queues. Currently implements RabbitMQ.

## Documentation

API docs can be found at https://informatics.pages.mdcatapult.io/klein/py-queue/src.

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
pip install -r requirements.txt
```

### Testing
```bash
docker-compose up
python -m pytest
```
## License
This project is licensed under the terms of the Apache 2 license, which can be found in the repository as `LICENSE.txt`