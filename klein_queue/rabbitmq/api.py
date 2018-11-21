# -*- coding: utf-8 -*-
import time
import requests
from klein_config import config

# TODO: Implement more robust distribiuted caching mechanism

ttl = config.get('rabbitmq.api.list_queues.cache', 0)
cache = {}


def list_queues(exchange, flush=False):
    '''
    utility to retrive queues attached to exchange
    configured user for connection shoudl have
    management permissions
    '''

    if flush:
        del cache[exchange]

    if exchange in cache:
        if "timestamp" in cache[exchange]:
            diff = time.time() - cache[exchange]["timestamp"]
            if diff >= ttl:
                del cache[exchange]
        if "queues" in cache[exchange]:
            return cache[exchange]["queues"]

    endpoint = "/api/exchanges/%%2f/%s/bindings/source" % exchange
    url = 'http://%s:%s%s' % (
        config.get("rabbitmq.host"),
        config.get("rabbitmq.management_port"),
        endpoint
    )

    response = requests.get(url, auth=(
        config.get("rabbitmq.username"),
        config.get("rabbitmq.password"))
    )
    queues = [q["destination"]
              for q in response.json() if q["destination_type"] == "queue"]

    cache[exchange] = {
        "queues": queues,
        "timestamp": time.time()
    }

    return queues
