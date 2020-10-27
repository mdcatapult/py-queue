# -*- coding: utf-8 -*-
"""
klein_queue.publisher
"""
import os
import logging
import json

LOGGER = logging.getLogger(__name__)


def requeue(publisher, config, data, on_limit_reached=None, **kwargs):
    """Requeue a message with a publisher.

    Convenience function handles requeue logic before publishing the
    given data with the given publisher.
    Executes callback on data if requeue limit is both found and exceeded.

    `publisher`: The `src.klein_queue.rabbitmq.publisher.Publisher` to requeue with.

    `config`: The instance of `klein_config.config.EnvironmentAwareConfig` with which to check the requeue limit.

    `data`: The message to requeue (a `dict`).

    `on_limit_reached`: Callback to execute on data if requeue limit is reached.
    """
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
        publisher.publish(data)
