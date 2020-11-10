import datetime
import json
import logging
from traceback import format_tb
import pika
from ..errors import KleinQueueError

LOGGER = logging.getLogger(__name__)


def new_default_exception_handler():
    def handler(exception, nack, basic_deliver=None, **kwargs):  # pylint: disable=unused-argument
        LOGGER.info("Exception occurred during processing of message # %s", basic_deliver.delivery_tag)
        requeue = False
        if isinstance(exception, KleinQueueError):
            requeue = exception.requeue
        nack(requeue)
    return handler


def new_retry_exception_handler(upstream, max_retries=3, on_limit_reached=None):
    def handler(exception, nack, body=None, properties=None, basic_deliver=None):
        if properties is None:
            properties = pika.BasicProperties(headers=dict())
        if properties.headers is None:
            properties.headers = dict()
        try:
            num_retries = properties.headers['x-retry']
        except KeyError:
            num_retries = 0
        if num_retries < max_retries:
            LOGGER.info("Requeuing message # %s, exception occurred during processing", basic_deliver.delivery_tag)
            properties.headers['x-retry'] = num_retries + 1
            upstream.publish(json.loads(body), properties)
        else:
            if on_limit_reached is not None:
                on_limit_reached(exception, body=body, properties=properties, basic_deliver=basic_deliver)
            else:
                LOGGER.info("Nacking message # %s, requeue limit reached", basic_deliver.delivery_tag)
        nack(False)

    return handler


def new_error_publishing_exception_handler(consumer_name, upstream, errors, max_retries=3):
    def on_limit_reached(exception, body=None, basic_deliver=None, **kwargs):  # pylint: disable=unused-argument
        LOGGER.info("Nacking and publishing exception info for message # %s, requeue limit reached", basic_deliver.delivery_tag)

        headers = {
            'x-consumer': consumer_name,
            'x-datetime': datetime.datetime.now().isoformat(),
            "x-exception": str(type(exception)),
            "x-message": str(exception),
            "x-queue": upstream.queue,
            "x-stack-trace": "\n".join(format_tb(exception.__traceback__))
        }

        errors.publish(
            json.loads(body),
            pika.BasicProperties(headers=headers, content_type='application/json')
        )

    return new_retry_exception_handler(upstream, max_retries=max_retries, on_limit_reached=on_limit_reached)
