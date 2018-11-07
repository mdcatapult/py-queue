# -*- coding: utf-8 -*-
import requests
from klein_config import config


def list_queues(exchange):
    '''
    utility to retrive queues attached to exchange
    configured user for connection shoudl have
    management permissions
    '''
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
    return queues
