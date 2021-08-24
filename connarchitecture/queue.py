from queue import Queue
from threading import Lock


class ConnectorQueue(Queue):
    def __init__(self):
        Queue.__init__(self)
        self._lock = Lock()

    def head(self, k=5):
        result = "["
        with self._lock:
            d = self.queue
            sep = ''
            for i in range(min(len(d), k)):
                result += sep
                result += f"{d[i]}"
                sep = ', '
        result += "]"

        return result

    def put(self, *args, **kwargs):
        super().put(*args, **kwargs)

    def get(self, *args, **kwargs):
        return super().get(*args, **kwargs)
