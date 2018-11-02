# -*- coding: utf-8 -*-
'''
klein_queue.rabbitmq.publisher
'''
import logging
import argparse
from klein_config import config
from .sync.publisher import Publisher


parser = argparse.ArgumentParser()
parser.add_argument("--debug", help="enable debug", action="store_true")
args, unknown = parser.parse_known_args()

LOGGER = logging.getLogger(__name__)

downstream = Publisher(config["publisher"])
downstream.connect()

upstream = Publisher(config["consumer"])
upstream.connect()

error = Publisher(config['error'])
error.connect()


def publish(message):
    '''
    publish message to downstream queue
    '''
    downstream.publish(message)


def requeue(message):
    '''
    publish message to same queue being consumed
    '''
    upstream.publish(message)
