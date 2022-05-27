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

class KleinQueueError(Exception):
    """Queue Error Class

    Raise a KleinQueueError inside your handler function to negatively acknowledge the message.
    Set the value of `requeue` to true if you want to requeue the message.
    If `requeue` is false, the `src.klein_queue.rabbitmq.consumer.Consumer` will call it's exception handler function if
    it has been set, otherwise the message will be negatively acknowledged and not requeued.
    """
    def __init__(self, *args, requeue=False):
        self.requeue = requeue
        self.msg = "KleinQueueError unknown"
        if len(args) > 0:
            self.msg = args[0]
        super().__init__(self, *args)

    def __str__(self):
        return str(self.msg)
