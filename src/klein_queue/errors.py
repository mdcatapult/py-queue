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
