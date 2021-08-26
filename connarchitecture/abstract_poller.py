from connarchitecture.abstract_worker import AbstractWorker
from connarchitecture.decorators import overrides
from connarchitecture.models import PollerResult
from connarchitecture.exceptions import PollerException, FatalException
from connarchitecture.constants import Constants
from abc import abstractmethod
from threading import Lock


class AbstractPoller(AbstractWorker):
    _static_init = False
    _thread_count = 0
    _lock = Lock()

    def __init__(self, name, **kwargs):
        AbstractWorker.__init__(self, name)
        with AbstractPoller._lock:
            AbstractPoller._thread_count += 1

    def set_out_queue(self, queue):
        self._out_queue = queue

    def set_topic(self, topic):
        self._topic = topic

    @overrides(AbstractWorker)
    def _prepare_for_run(self):
        if not self._out_queue:
            raise FatalException(message="No output queue set in poller")

        with AbstractPoller._lock:
            if not AbstractPoller._static_init:
                try:
                    self.static_initialize()
                except Exception as e:
                    raise FatalException() from e
                AbstractPoller._static_init = True

        try:
            self.initialize()
        except Exception as e:
            raise FatalException() from e

    @overrides(AbstractWorker)
    def _step(self):
        polled_result = None
        poll_reference = None
        success = False
        try:
            with AbstractPoller._lock:
                result = self.poll()
                if result:
                    polled_result, poll_reference, success = result
                if not success and poll_reference:
                    self.update(Constants.EVENT_ERROR, poll_reference)
        except FatalException as e:
            raise e
        except Exception as e:
            raise PollerException() from e
        else:
            self._handle_result(polled_result, poll_reference)

    def _handle_result(self, polled_result, poll_reference):
        if polled_result:
            poller_result = PollerResult(result=polled_result, poll_reference=poll_reference)
            self._out_queue.put(poller_result)
            self.update(Constants.EVENT_POLLED, poll_reference)

    @overrides(AbstractWorker)
    def _cleanup(self):
        try:
            self.cleanup()
            with AbstractPoller._lock:
                AbstractPoller._thread_count -= 1
                if AbstractPoller._thread_count == 0:
                    self.static_cleanup()
        except FatalException as e:
            raise FatalException() from e
        except Exception as e:
            raise PollerException() from e

    @abstractmethod
    def static_initialize(self):
        """
        Override this method with initialization code that needs to be executed only once for all poller threads.
        """
        pass

    @abstractmethod
    def initialize(self):
        """
        Override this method with initialization code that needs to be executed for each thread.
        """
        pass

    @abstractmethod
    def poll(self):
        """
        Poll process. Represent one poll cycle.
        :return the poll result
        """
        pass

    @abstractmethod
    def cleanup(self):
        """
        Override this method with cleanup code that needs to be executed for each thread. Called on exit.
        """
        pass

    @abstractmethod
    def static_cleanup(self):
        """
        Override this method with cleanup code that needs to be executed only once for all poller threads when the last thread exits.
        """
        pass
