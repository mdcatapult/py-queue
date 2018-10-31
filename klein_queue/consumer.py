# -*- coding: utf-8 -*-
from .rabbitmq.consumer import consume as QueueConsume
import json

def consume(fn):
    QueueConsume(fn)