# -*- coding: utf-8 -*-
'''
klein_queue.publisher
'''
import os
import logging
import json
from klein_config import config
from .rabbitmq.publisher import publish as QueuePublish
from .rabbitmq.publisher import requeue as QueueRequeue
from .rabbitmq.publisher import error as QueueError


LOGGER = logging.getLogger(__name__)


def publish(data):
    '''
    Publish data to configured queue
    '''
    QueuePublish(data)


def requeue(data, **kwargs):
    '''
    publish data back on to queue being consumed
    '''
    if "klein.requeued" in data:
        data["klein.requeued"] = data["klein.requeued"] + 1
    else:
        data["klein.requeued"] = 1

    limit = kwargs.get("limit", int(config["limits"]["max_requeue"]))

    if limit and int(data["klein.requeued"]) >= limit:
        if "consumer" in config and "queue" in config["consumer"]:
            data["queue"] = config["consumer"]["queue"]
        data["path"] = os.getcwd()
        data["message"] = f"Max Requeue limit ({limit}) hit"
        LOGGER.error('Requeue Error: %s', json.dumps(data))
        QueueError(data)
    else:
        QueueRequeue(data)


def error(data):
    '''
    publish data to error queue
    '''
    LOGGER.debug(json.dumps(data))
    QueueError(data)
