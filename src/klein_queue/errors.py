class KleinQueueError(Exception):
    """Queue Error Class

    Raise a KleinQueueError inside your handler function to negatively acknowledge the message.
    Otherwise the message will be positively acknowledged.
    Set the value of `requeue` to true if you want to requeue the message.
    Set the `body` of the error
    """
    def __init__(self, *args, requeue=False, body=None):
        self.body = body
        self.requeue = requeue
        self.msg = "KleinQueueError unknown"
        if len(args) > 0:
            self.msg = args[0]
        super().__init__(self, *args)

    def __str__(self):
        return str(self.msg)
