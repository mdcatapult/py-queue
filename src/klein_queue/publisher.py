# -*- coding: utf-8 -*-
'''
klein_queue.publisher
'''
import os
import logging
import json
from .rabbitmq.publisher import publish

LOGGER = logging.getLogger(__name__)


def rabbit_publish(config, key, data):
    '''
    Publish data to queue defined on config key
    '''
    publish(config, key, data)


def rabbit_requeue(config, key, data, on_limit_reached=None, **kwargs):
    '''
    publishes data to queue defined on config key.
    executes callback on data if requeue limit is both found and exceeded.

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
        publish(config, key, data)
