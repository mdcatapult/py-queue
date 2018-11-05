'''
klein_queue.rabbit
'''

from .consumer import consume
from .api import list_queues
from .publisher import publish
from .publisher import requeue
from .publisher import error
