# -*- coding: utf-8 -*-
'''
klein_queue_rabbitmq.consumer
'''
from klein_config import config
from .asynchronous.consumer import Consumer


def consume(callback):
    '''
    use auto detected config from klein_config to instantiate consumer
    '''
    c = Consumer(config.get("consumer"), callback)
    try:
        c.run()
    except KeyboardInterrupt:
        c.stop()
