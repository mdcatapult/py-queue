# -*- coding: utf-8 -*-
'''
klein_queue.rabbitmq.publisher
'''
from klein_config import config
from .sync.publisher import Publisher


DOWNSTREAM = Publisher(config["publisher"])
DOWNSTREAM.connect()

UPSTREAM = Publisher(config["consumer"])
UPSTREAM.connect()

ERROR = Publisher(config['error'])
ERROR.connect()


def publish(message):
    '''
    publish message to downstream queue
    '''
    DOWNSTREAM.publish(message)


def requeue(message):
    '''
    publish message to same queue being consumed
    '''
    UPSTREAM.publish(message)

def error(message):
    '''
    publish message to error queue
    '''
    ERROR.publish(message)