from connarchitecture.abstract_worker import AbstractWorker
from connarchitecture.decorators import overrides
from connarchitecture.exceptions import SenderException, FatalException
from connarchitecture.constants import Constants
from abc import abstractmethod
from threading import Lock
from queue import Empty


class AbstractSender(AbstractWorker):
    _static_init = False
    _thread_count = 0
    _lock = Lock()

    def __init__(self, name, **kwargs):
        AbstractWorker.__init__(self, name)
        with AbstractSender._lock:
            AbstractSender._thread_count += 1

    def set_in_queue(self, queue):
        self._in_queue = queue

    @overrides(AbstractWorker)
    def _prepare_for_run(self):
        if not self._in_queue:
            raise FatalException(message="No input queue set in parser")

        with AbstractSender._lock:
            if not AbstractSender._static_init:
                try:
                    self.static_initialize()
                except Exception as e:
                    raise FatalException() from e
                AbstractSender._static_init = True

        try:
            self.initialize()
        except Exception as e:
            raise FatalException() from e

    @overrides(AbstractWorker)
    def _step(self):
        try:
            parser_result = self._in_queue.get(block=False)
            poll_reference = parser_result.get_poll_reference()
        except Empty:
            pass
        else:
            '''
            The use of the else clause is better than adding additional code to the try 
            clause because it avoids accidentally catching an exception that wasn’t raised 
            by the code being protected by the try ... except statement
            '''
            try:
                if parser_result.get_result():
                    self.process(parser_result.get_result(), poll_reference)
                    self.update(Constants.EVENT_SEND, poll_reference)
            except FatalException as e:
                raise FatalException(poll_reference=poll_reference) from e
            except Exception as e:
                raise SenderException(poll_reference=poll_reference) from e

    @overrides(AbstractWorker)
    def _cleanup(self):
        try:
            self.cleanup()
            with AbstractSender._lock:
                AbstractSender._thread_count -= 1
                if AbstractSender._thread_count == 0:
                    self.static_cleanup()
        except FatalException as e:
            raise FatalException() from e
        except Exception as e:
            raise SenderException() from e

    @abstractmethod
    def static_initialize(self):
        """
        Override this method with initialization code that needs to be executed only once for all sender threads.
        """
        pass

    @abstractmethod
    def initialize(self):
        """
        Override this method with initialization code that needs to be executed for each thread.
        """
        pass

    @abstractmethod
    def process(self, input, poll_reference=None):
        """
        Sender process.
        :input message The message to process.
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
        Override this method with cleanup code that needs to be executed only once for all sender threads when the last thread exits.
        """
        pass
