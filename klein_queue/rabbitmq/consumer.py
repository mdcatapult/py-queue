# -*- coding: utf-8 -*-
from klein_config import config
from .async.consumer import Consumer
import logging
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--debug", help="enable debug", action="store_true")
args, unknown = parser.parse_known_args()

LOGGER = logging.getLogger(__name__)


def consume(fn):
    c = Consumer(config["consumer"], fn)
    try:
        c.run()
    except KeyboardInterrupt:
        c.stop()
