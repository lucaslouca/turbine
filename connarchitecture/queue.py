from queue import Queue
from threading import Lock
from queue import Empty


class ConnectorQueue:
    def __init__(self):
        self._topic_queues = {}
        self._default_queue = Queue()
        self._lock = Lock()

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
