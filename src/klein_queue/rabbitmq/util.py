import random
import pika


class KleinQueueError(Exception):
    '''
    Queue Error Class
    '''
    def __init__(self, *args, body=None):
        self.body = body
        self.msg = "KleinQueueError unknown"
        if len(args) > 0:
            self.msg = args[0]
        super().__init__(self, *args)

    def __str__(self):
        return str(self.msg)


def get_url_parameters(conf):
    conns = []
    hosts = conf.get("rabbitmq.host")

    if isinstance(hosts, str):
        hosts = hosts.split(",")

    random.shuffle(hosts)

    authority = '%s:%s@' % (
        conf.get("rabbitmq.username"),
        conf.get("rabbitmq.password")
    )
    if authority == ':@':
        authority = ''

    url = 'amqp://%s%s:%s/' % (
        authority,
        hosts[0],
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
