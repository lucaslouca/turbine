from connarchitecture.logging_component import LoggingComponent
from connarchitecture.exceptions import ProcessException
from connarchitecture.models import Transaction
import threading
from abc import ABC, abstractmethod
import time
from queue import Empty


class AbstractWorker(ABC, threading.Thread, LoggingComponent):
    def __init__(self, name):
        threading.Thread.__init__(self)
        LoggingComponent.__init__(self, name)
        self._transaction_queue = None
        self._continue = True

    def run(self):
        try:
            self._prepare_for_run()

            while self._continue:
                self._step()
                time.sleep(0.1)

            self._cleanup()

        except ProcessException as e:
            self.throw(e, e.get_poll_reference())

    def stop(self):
        self._continue = False

    @abstractmethod
    def _prepare_for_run(self):
        pass

    @abstractmethod
    def _step(self):
        pass

    @abstractmethod
    def _cleanup(self):
        pass
