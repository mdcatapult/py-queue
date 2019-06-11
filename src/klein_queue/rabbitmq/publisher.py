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
    q.connect()

if config.has("publisher"):
    DOWNSTREAM = Publisher(config.get("publisher"))
    try:
        c(DOWNSTREAM)
    except pika.exceptions.ConnectionClosed:
        c(DOWNSTREAM)

if config.has("consumer"):
    UPSTREAM = Publisher(config.get("consumer"))
    try:
        c(UPSTREAM)
    except pika.exceptions.ConnectionClosed:
        c(UPSTREAM)

if config.has("error"):
    ERROR = Publisher(config.get('error'))
    try:
        c(ERROR)
    except pika.exceptions.ConnectionClosed:
        c(ERROR)


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
