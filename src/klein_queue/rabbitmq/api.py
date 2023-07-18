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

# -*- coding: utf-8 -*-
import time
import requests


class ApiClient:

    def __init__(self, config):
        self._config = config
        self._cache = {}

    def list_queues(self, exchange, flush=False, timeout=None):
        """Utility to retrieve queues attached to exchange
        configured user for connection should have
        management permissions.

        `exchange`: `str` the exchange to query.
        `flush`: `bool` whether or not to flush the results cache.
        `timeout`: `int` the time to wait for the request
        """

        if flush:
            del self._cache[exchange]

        if exchange in self._cache and "timestamp" in self._cache[exchange]:
            diff = time.time() - self._cache[exchange]["timestamp"]
            if diff >= self._config.get('rabbitmq.api.list_queues.cache', 0):
                del self._cache[exchange]

        if exchange in self._cache and "queues" in self._cache[exchange]:
            return self._cache[exchange]["queues"]

        host = self._config.get("rabbitmq.host")
        if isinstance(host, list):
            host = host[0]

        # TODO: Implement other vhosts than default.
        endpoint = f"/api/exchanges/%%2f/{exchange}/bindings/source"
        url = f'http://{host}:{self._config.get("rabbitmq.management_port")}{endpoint}'

        response = requests.get(url,
                                auth=(self._config.get("rabbitmq.username"),
                                      self._config.get("rabbitmq.password")),
                                timeout=timeout)
        queues = [q["destination"]
                  for q in response.json() if q["destination_type"] == "queue"]

        self._cache[exchange] = {
            "queues": queues,
            "timestamp": time.time()
        }

        return queues
