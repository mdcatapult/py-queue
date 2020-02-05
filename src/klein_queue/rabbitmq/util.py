import random
import pika


class KleinQueueError(Exception):
    '''
    Doclib Error Class
    '''


def get_url_parameters(conf):
    conns = []
    hosts = conf.get("rabbitmq.host")

    if isinstance(hosts, str):
        hosts = hosts.split(",")

    random.shuffle(hosts)

    for ii in range(len(hosts)):
        url = 'amqp://%s:%s@%s:%s/' % (
            conf.get("rabbitmq.username"),
            conf.get("rabbitmq.password"),
            hosts[ii],
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
