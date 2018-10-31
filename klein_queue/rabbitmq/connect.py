# -*- coding: utf-8 -*-
from klein_config import config as common_config
import pika
import argparse
import logging

parser = argparse.ArgumentParser()
parser.add_argument("--debug", help="enable debug", action="store_true")
args, unknown = parser.parse_known_args()

LOGGER = logging.getLogger(__name__)


def blocking():
    url = 'amqp://%s:%s@%s:%s/?backpressure_detection=t' % (
        common_config["rabbitmq"]["username"],
        common_config["rabbitmq"]["password"],
        common_config["rabbitmq"]["host"],
        common_config["rabbitmq"]["port"])
    params = pika.URLParameters(url)
    params.socket_timeout = 5
    LOGGER.debug("RabbitMQ - Connecting: %s ")
    return pika.BlockingConnection(params)
