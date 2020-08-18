# -*- coding: utf-8 -*-
'''
klein_queue.consumer
'''
from .rabbitmq.consumer import consume


def rabbit_consume(config, key, callback):
    '''
    Consume from rabbit queue with given key in config.
    Execute callback on message.
    '''
    consume(config, key, callback)
