import sys


def setup():
    sys.argv = [' ', '--config', '/home/simon.skinner@medcat.local/Projects/klein/py-queue/config.yml']


def test_connection():
    from src.klein_queue.rabbitmq.asynchronous.connect import Connection
    c = Connection(config={})
    c.connect()
