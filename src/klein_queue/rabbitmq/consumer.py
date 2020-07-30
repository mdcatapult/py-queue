# -*- coding: utf-8 -*-
'''
klein_queue_rabbitmq.consumer
'''
import time

from .asynchronous.consumer import Consumer


def nack(msg):
    '''
    Convenience currid function to Negative Acknowledge message
    '''

    def handle(consumer, channel, envelope, properties):
        # pylint: disable=unused-argument
        print(time.time(), "NACK: ", envelope.delivery_tag, msg)
        channel.basic_nack(envelope.delivery_tag)

    return handle


def ack(msg):
    '''
    Convenience curried function to Acknowledge message
    '''

    def handle(consumer, channel, envelope, properties):
        # pylint: disable=unused-argument
        print(time.time(), "ACK: ", envelope.delivery_tag, msg)
        channel.basic_ack(envelope.delivery_tag)

    return handle


def nackError(err):
    '''
    Convenience curried function to Negative Acknowledge message with error
    '''

    def handle(consumer, channel, basic_deliver, properties):
        # pylint: disable=unused-argument
        print(time.time(), "ERROR: ", str(err))
        channel.basic_nack(basic_deliver.delivery_tag)

    return handle


def consume(config, callback):
    '''
    use auto detected config from klein_config to instantiate consumer
    '''
    c = Consumer(config, callback)
    try:
        c.run()
    except KeyboardInterrupt:
        c.stop()
