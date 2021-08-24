from connarchitecture.logging_component import LoggingComponent
from connarchitecture.exceptions import ProcessException
from queue import Empty
from abc import ABC, abstractmethod
import threading
import time


class AbstractTransactionHandler(ABC, threading.Thread, LoggingComponent):
    def __init__(self):
        threading.Thread.__init__(self)
        LoggingComponent.__init__(self, self.component_name())
        self._transaction_queue = None
        self._continue = True

    def run(self):
        self.log("Started")
        while self._continue:
            try:
                transaction = self._transaction_queue.get(block=False)
                self.on_event(transaction.object, transaction.success)
            except Empty:
                pass
            time.sleep(0.1)

    def set_transaction_queue(self, queue):
        self._transaction_queue = queue

    def stop(self):
        self._continue = False

    def component_name(self):
        return "AbstractTransactionHandler"

    @abstractmethod
    def on_event(self, object, success):
        pass
