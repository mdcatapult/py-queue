# -*- coding: utf-8 -*-
'''
klein_queue.consumer
'''
from .rabbitmq.consumer import consume


def consume_rabbit_queue(config, callback):
    '''
    Consumer configured queue with callback
    '''
    consume(config, callback)
