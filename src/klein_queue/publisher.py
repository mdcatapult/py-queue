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

LOGGER = logging.getLogger(__name__)


def publish(data):
    '''
    Publish data to configured queue
    '''
    QueuePublish(data)


def requeue(data, on_limit_reached=None, **kwargs):
    '''
    publish data back on to queue being consumed

    :keyword on_limit_reached -- Callback to execute if requeue limit is reached.
    '''
    if "klein.requeued" in data:
        data["klein.requeued"] = data["klein.requeued"] + 1
    else:
        data["klein.requeued"] = 1

    limit = kwargs.get("limit", int(config.get("limits.max_requeue", False)))

    if limit and int(data["klein.requeued"]) >= limit:
        if config.has("consumer.queue"):
            data["queue"] = config.get("consumer.queue")
        data["path"] = os.getcwd()
        data["message"] = f"Max Requeue limit ({limit}) hit"
        LOGGER.error('Requeue Error: %s', json.dumps(data))
        if on_limit_reached:
            on_limit_reached(data)
    else:
        QueueRequeue(data)
