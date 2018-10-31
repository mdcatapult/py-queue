# -*- coding: utf-8 -*-
from klein_config import config
from .rabbitmq.publisher import publish as QueuePublish, requeue as QueueRequeue, error as QueueError
import os
import logging  
import json


LOGGER = logging.getLogger(__name__)

def publish(data):
    QueuePublish(data)


def requeue(data, **kwargs):
    if "klein.requeued" in data:
        data["klein.requeued"] = data["klein.requeued"] + 1
    else:
        data["klein.requeued"] = 1
    
    limit = int(config["limits"]["max_requeue"])
    if "limit" in kwargs: 
        limit = kwargs["limit"]

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
    LOGGER.debug(json.dumps(data))
    QueueError(data)
