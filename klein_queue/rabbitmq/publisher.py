# -*- coding: utf-8 -*-
'''
klein_queue.rabbitmq.publisher
'''
from klein_config import config
from .synchronous.publisher import Publisher

DOWNSTREAM = None
UPSTREAM = None
ERROR = None

if config.has("publisher"):
  DOWNSTREAM = Publisher(config.get("publisher"))
  DOWNSTREAM.connect()

if config.has("consumer"):
  UPSTREAM = Publisher(config.get("consumer"))
  UPSTREAM.connect()

if config.has("error"):
  ERROR = Publisher(config.get('error'))
  ERROR.connect()


def publish(message):
    '''
    publish message to downstream queue
    '''
    if not DOWNSTREAM:
      raise EnvironmentError("No downstream has been configured for publishing")
    DOWNSTREAM.publish(message)


def requeue(message):
    '''
    publish message to same queue being consumed
    '''
    if not UPSTREAM:
      raise EnvironmentError("No upstream has been configured for publishing")
    UPSTREAM.publish(message)


def error(message):
    '''
    publish message to error queue
    '''
    if not ERROR:
      raise EnvironmentError("No error has been configured for publishing")
    ERROR.publish(message)
