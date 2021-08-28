from queue import Queue
from threading import Lock
from queue import Empty


class ConnectorQueue:
    def __init__(self):
        self._topic_queues = {}
        self._default_queue = Queue()
        self._lock = Lock()

    def _stringify_queue(self, name, q, k):
        result = f" {name}: ["
        d = q.queue
        sep = ''
        for i in range(min(len(d), k)):
            result += sep
            result += f"{d[i]}"
            sep = ', '
        result += "]\n"
        return result

    def head(self, k=5):
        result = self._stringify_queue(name='default', q=self._default_queue, k=k)

        for topic in self._topic_queues:
            result += self._stringify_queue(name=topic, q=self._topic_queues[topic], k=k)

        return result

    def put(self, *args, **kwargs):
        self._default_queue.put(*args, **kwargs)

    def get(self, *args, **kwargs):
        return self._default_queue.get(*args, **kwargs)

    def put_topic(self, topic, *args, **kwargs):
        with self._lock:
            if not topic in self._topic_queues:
                self._topic_queues[topic] = Queue()
        self._topic_queues[topic].put(*args, **kwargs)

    def get_topic(self, topic, *args, **kwargs):
        if topic in self._topic_queues:
            return self._topic_queues[topic].get(*args, **kwargs)
        else:
            raise Empty
