class KleinQueueError(Exception):
    '''
    Queue Error Class
    '''
    def __init__(self, *args, body=None):
        self.body = body
        self.msg = "KleinQueueError unknown"
        if len(args) > 0:
            self.msg = args[0]
        super().__init__(self, *args)

    def __str__(self):
        return str(self.msg)
