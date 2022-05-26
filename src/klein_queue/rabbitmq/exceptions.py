# copyright 2022 Medicines Discovery Catapult
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import json
import logging
from traceback import format_tb
import pika

LOGGER = logging.getLogger(__name__)


def new_retry_exception_handler(upstream, max_retries=3, on_limit_reached=None):
    """Returns a retry exception handler.

    `upstream`: The upstream `src.klein_queue.rabbitmq.publisher.Publisher` to publish retries with.

    `max_retries`: The `int` maximum number of retries.

    `on_limit_reached`: A callback function to be executed when `max_retries` has been exceeded.

    The retry exception handler checks the value of the `x-retry` message header (or creates one with value 0).
    If the number of retries is less than `max_retries`, a new message is published to the `upstream` publisher
    which contains the incremented `x-retry` header value and the original message body. If the number of retries
    is greater than `max_retries`, the retry exception handler will call `on_limit_reached` if it has been set.

    **The original message is always negatively acknowledged and is not requeued**. It is not possible to manipulate
    headers when negatively acknowledging and requeuing in the normal way.
    """
    def handler(exception, nack, body=None, properties=None, basic_deliver=None):
        if properties is None:
            properties = pika.BasicProperties(headers={})
        if properties.headers is None:
            properties.headers = {}
        try:
            num_retries = properties.headers['x-retry']
        except KeyError:
            num_retries = 1
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
    """Returns an error publishing exception handler.

    `consumer_name`: The `string` name of the consumer. This is added to the headers of published error messages.

    `upstream`: The upstream `src.klein_queue.rabbitmq.publisher.Publisher` to publish retries with.

    `errors`: The errors `src.klein_queue.rabbitmq.publisher.Publisher` to publish errors with.

    `max_retries`: The `int` maximum number of retries.

    The error publishing exception handler calls `src.klein_queue.rabbitmq.exceptions.new_retry_exception_handler` and
    passes in a callback to be executed when the retry limit is reached. The callback publishes a message with the
    `errors` publisher containing the original message body, and with headers containing information extracted from the
    exception.
    """
    def on_limit_reached(exception, body=None, basic_deliver=None, **kwargs):  # pylint: disable=unused-argument
        LOGGER.info("Nacking and publishing exception info for message # %s, requeue limit reached",
                    basic_deliver.delivery_tag)

        headers = {
            'x-consumer': consumer_name,
            'x-datetime': datetime.datetime.now().isoformat(),
            "x-exception": str(type(exception)),
            "x-message": str(exception),
            "x-queue": upstream.queue,
            "x-stack-trace": "\n".join(format_tb(exception.__traceback__)),
            "x-original-exchange": basic_deliver.exchange,
            "x-original-routing-key": basic_deliver.routing_key
        }

        errors.publish(
            json.loads(body),
            pika.BasicProperties(headers=headers, content_type='application/json')
        )

    return new_retry_exception_handler(upstream, max_retries=max_retries, on_limit_reached=on_limit_reached)
