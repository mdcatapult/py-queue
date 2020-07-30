# -*- coding: utf-8 -*-
'''
klein_queue.publisher
'''
import os
import logging
import json
from .rabbitmq.publisher import publish

LOGGER = logging.getLogger(__name__)


def publish_downstream(config, data):
    '''
    synchronously publishes data to "publisher"

    NOTE: This is a convenience function which creates a new connection on every call.
    Use an instance of the Publisher for a persistent connection to reduce overhead.
    '''
    publish(config.get("publisher"), data)


def requeue(config, data, on_limit_reached=None, **kwargs):
    '''
    synchronously publishes data back to "consumer" and executes a callback if a requeue limit can
    be found is exceeded.

    :keyword on_limit_reached -- Callback to execute on data if requeue limit is reached.
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
        publish(config.get("consumer"), data)
