# -*- coding: utf-8 -*-
from klein_config import config
from .sync.publisher import Publisher
import logging
import argparse

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
    downstream.publish(message)


def requeue(message):
    upstream.publish(message)

