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
        success = False
    return success


def publish(queue_config, message):
    '''
    synchronously publishes a message to a queue with the given configuration

    NOTE: This is a convenience function which creates a new connection on every call.
    Use an instance of the Publisher for a persistent connection to reduce overhead.
    '''
    publisher = Publisher(queue_config)
    connected = False
    while not connected:
        connected = connect(publisher)
    publisher.publish(message)

