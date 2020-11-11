"""
Package `src.klein_queue.rabbitmq` provides a user friendly API which abstracts over the
(much less friendly) [pika](https://github.com/pika/pika) library for python.
"""

from .api import ApiClient
from .consumer import Consumer
from .publisher import Publisher
