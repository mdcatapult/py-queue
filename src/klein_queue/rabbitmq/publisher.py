# -*- coding: utf-8 -*-
'''
klein_queue.rabbitmq.publisher
'''
import logging
import pika.exceptions
from .synchronous.publisher import Publisher

LOGGER = logging.getLogger(__name__)


def connect(q):
    success = False
    try:
        LOGGER.debug("QUEUE: Attempting Connection to %s", q._url if hasattr(q, "_url") else "unknown")
        q.connect()
        success = True
    except pika.exceptions.ConnectionClosed:
        LOGGER.debug("QUEUE: Connection Failed for %s", q._url if hasattr(q, "_url") else "unknown")
    return success


def publish(config, key, message, properties=None):
    '''
    Publish message to queue with given key in the config.

    NOTE: This is a convenience function. Each call will create a new connection to rabbit.
    Use the Publisher class for a persistent connection.
    '''
    if config.has(key):
        queue = Publisher(config, key)
    else:
        raise EnvironmentError(
            "No downstream has been configured for publishing")

    connected = False
    while not connected:
        connected = connect(queue)

    queue.publish(message, properties)
