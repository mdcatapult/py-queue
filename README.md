# Klein Queue

Module to abstract queues. Currently only implements RabbitMQ, roadmap to include alternatives such as Kafka.

## Environment Variables


| Env Variable                        | Description |
|-------------------------------------|-------------|
| RABBITMQ_USERNAME                   |             |
| RABBITMQ_PASSWORD                   |             |
| RABBITMQ_HOST                       |             |
| RABBITMQ_PORT                       |             |
| RABBITMQ_VHOST                      |             |
| RABBITMQ_SOCKET_TIMEOUT             |             |
| RABBITMQ_HEARTBEAT                  |             |
| RABBITMQ_BLOCKED_CONNECTION_TIMEOUT |             |
| RABBITMQ_RETRY_DELAY                |             |
| RABBITMQ_PUBLISHER                  |             |
| RABBITMQ_CONSUMER                   |             |
| RABBITMQ_ERROR                      |             |

## Python

Utilises python 3.7

### Ubuntu

```
sudo apt install python3.7
```

## Virtualenv

```
virtualenv -p python3.7 venv
```
