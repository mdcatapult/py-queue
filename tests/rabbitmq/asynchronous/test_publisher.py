import sys


def setup():
    sys.argv = [' ', '--config', '/home/simon.skinner@medcat.local/Projects/klein/py-queue/config.yml']


def test_connection():
    from src.klein_queue.rabbitmq.asynchronous.publisher import Publisher
    c = Publisher(config={})
    c.connect()


def test_publish_message():
    from src.klein_queue.rabbitmq.asynchronous.publisher import Publisher
    c = Publisher(config={"queue": "klein.prefetch.publish"})
    c.run()
    c.publish_message()
    c.stop()
