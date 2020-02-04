import random
import pika


def get_url_parameters(conf):
    conns = []
    if isinstance(conf.get("rabbitmq.host"), str):
        conf["rabbitmq.host"] = list(conf.get("rabbitmq.host"))

    random.shuffle(conf.get("rabbitmq.host"))

    for ii in range(len(conf.get("rabbitmq.host"))):
        url = 'amqp://%s:%s@%s:%s/' % (
            conf.get("rabbitmq.username"),
            conf.get("rabbitmq.password"),
            conf.get("rabbitmq.host")[ii],
            conf.get("rabbitmq.port"))
        connection_params = pika.URLParameters(url)
        connection_params._virtual_host = conf.get("rabbitmq.vhost", "/")
        connection_params.socket_timeout = conf.get(
            "rabbitmq.socket_timeout", 5)
        connection_params.heartbeat = conf.get(
            "rabbitmq.heartbeat", 120)
        connection_params.blocked_connection_timeout = conf.get(
            "rabbitmq.blocked_connection_timeout", 300)
        connection_params.retry_delay = conf.get(
            "rabbitmq.retry_delay", 10)
        conns.append(connection_params)

    return conns
