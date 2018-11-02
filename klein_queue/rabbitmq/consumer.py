# -*- coding: utf-8 -*-
import logging
import argparse
from klein_config import config
from .async.consumer import Consumer


def consume(callback):
    '''
    use auto detected config from klein_config to instantiate consumer
    '''
    c = Consumer(config["consumer"], callback)
    try:
        c.run()
    except KeyboardInterrupt:
        c.stop()
