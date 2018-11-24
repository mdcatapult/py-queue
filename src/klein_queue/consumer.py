# -*- coding: utf-8 -*-
'''
klein_queue.consumer
'''
from .rabbitmq.consumer import consume as QueueConsume


def consume(callback):
    '''
    Consumer configured queue with callback
    '''
    QueueConsume(callback)
