# -*- coding: utf-8 -*-
import time
import requests

# TODO: Implement more robust distributed caching mechanism


class ApiClient:

    def __init__(self, config):
        self._ttl = config.get('rabbitmq.api.list_queues.cache', 0)
        self._config = config
        self._cache = {}

    def list_queues(self, exchange, flush=False):
        """
        utility to retrieve queues attached to exchange
        configured user for connection should have
        management permissions
        """

        if flush:
            del self._cache[exchange]

        if exchange in self._cache and "timestamp" in self._cache[exchange]:
            diff = time.time() - self._cache[exchange]["timestamp"]
            if diff >= self._ttl:
                del self._cache[exchange]

        if exchange in self._cache and "queues" in self._cache[exchange]:
            return self._cache[exchange]["queues"]

        host = self._config.get("rabbitmq.host")
        if isinstance(host, list):
            host = host[0]

        # TODO: Implement other vhosts than default.
        endpoint = "/api/exchanges/%%2f/%s/bindings/source" % exchange
        url = 'http://%s:%s%s' % (
            host,
            self._config.get("rabbitmq.management_port"),
            endpoint
        )

        response = requests.get(url, auth=(
            self._config.get("rabbitmq.username"),
            self._config.get("rabbitmq.password"))
        )
        queues = [q["destination"]
                  for q in response.json() if q["destination_type"] == "queue"]

        self._cache[exchange] = {
            "queues": queues,
            "timestamp": time.time()
        }

        return queues
