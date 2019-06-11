# -*- coding: utf-8 -*-
'''
klein_queue.rabbitmq.publisher
'''
import pika.exceptions
from klein_config import config
from .synchronous.publisher import Publisher


DOWNSTREAM = None
UPSTREAM = None
ERROR = None

def c(q):
    success = False
    try:
        print(f"QUEUE: Attempting Connection to ${q._url}")
        q.connect()
        success = True
    except pika.exceptions.ConnectionClosed:
        print(f"QUEUE: Connection Failed for ${q._url}")
        success = False
    return success

if config.has("publisher"):
    DOWNSTREAM = Publisher(config.get("publisher"))
    connected = False
    while not connected:
        connected = c(DOWNSTREAM)

if config.has("consumer"):
    UPSTREAM = Publisher(config.get("consumer"))
    connected = False
    while not connected:
        connected = c(DOWNSTREAM)

if config.has("error"):
    ERROR = Publisher(config.get('error'))
    connected = False
    while not connected:
        connected = c(DOWNSTREAM)


def publish(message):
    '''
    publish message to downstream queue
    '''
    if not DOWNSTREAM:
        raise EnvironmentError(
            "No downstream has been configured for publishing")
    DOWNSTREAM.publish(message)


def requeue(message):
    '''
    publish message to same queue being consumed
    '''
    if not UPSTREAM:
        raise EnvironmentError(
            "No upstream has been configured for publishing")
    UPSTREAM.publish(message)


def error(message):
    '''
    publish message to error queue
    '''
    if not ERROR:
        raise EnvironmentError("No error has been configured for publishing")
    ERROR.publish(message)
