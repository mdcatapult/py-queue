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
        config["rabbitmq"]["host"],
        config["rabbitmq"]["management_port"],
        endpoint
    )

    response = requests.get(url, auth=(
        config["rabbitmq"]["username"], config["rabbitmq"]["password"]))
    queues = [q["destination"]
              for q in response.json() if q["destination_type"] == "queue"]
    return queues
