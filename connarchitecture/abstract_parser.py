from connarchitecture.abstract_worker import AbstractWorker
from connarchitecture.decorators import overrides
from connarchitecture.models import ParserResult
from connarchitecture.exceptions import ParserException, FatalException
from connarchitecture.constants import Constants
from abc import abstractmethod
from threading import Lock
from queue import Empty


class AbstractParser(AbstractWorker):
    _static_init = False
    _thread_count = 0
    _lock = Lock()

    def __init__(self, name, **kwargs):
        AbstractWorker.__init__(self, name)
        with AbstractParser._lock:
            AbstractParser._thread_count += 1

    def set_in_queue(self, queue):
        self._in_queue = queue

    def set_out_queue(self, queue):
        self._out_queue = queue

    @overrides(AbstractWorker)
    def _prepare_for_run(self):
        if not self._in_queue:
            raise FatalException(message="No input queue set in parser")

        if not self._out_queue:
            raise FatalException(message="No output queue set in parser")

        with AbstractParser._lock:
            if not AbstractParser._static_init:
                try:
                    self.static_initialize()
                except Exception as e:
                    raise FatalException() from e
                AbstractParser._static_init = True

        try:
            self.initialize()
        except Exception as e:
            raise FatalException() from e

    @overrides(AbstractWorker)
    def _step(self):
        try:
            poller_result = self._in_queue.get(block=False)
            poll_reference = poller_result.get_poll_reference()
        except Empty:
            pass
        else:
            try:
                if poller_result.get_result():
                    parsed_result = self.parse(poller_result.get_result(), poll_reference)
                    if parsed_result:
                        self._handle_result(parsed_result, poll_reference)
                    else:
                        self.update(Constants.EVENT_DONE, poll_reference)
            except FatalException as e:
                raise FatalException(poll_reference=poll_reference) from e
            except Exception as e:
                raise ParserException(poll_reference=poll_reference) from e

    def _handle_result(self, parsed_result, poll_reference):
        # if parsed_result:
        parser_result = ParserResult(result=parsed_result, poll_reference=poll_reference)
        self._out_queue.put(parser_result)
        self.update(Constants.EVENT_PARSED, poll_reference)

    @overrides(AbstractWorker)
    def _cleanup(self):
        try:
            self.cleanup()
            with AbstractParser._lock:
                AbstractParser._thread_count -= 1
                if AbstractParser._thread_count == 0:
                    self.static_cleanup()
        except FatalException as e:
            raise FatalException() from e
        except Exception as e:
            raise ParserException() from e

    @abstractmethod
    def static_initialize(self):
        """
        Override this method with initialization code that needs to be executed only once for all parser threads.
        """
        pass

    @abstractmethod
    def initialize(self):
        """
        Override this method with initialization code that needs to be executed for each thread.
        """
        pass

    @abstractmethod
    def parse(self, input, poll_reference=None):
        """
        Parse process.
        :input message The message to parse.
        :return the parse result
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
        Override this method with cleanup code that needs to be executed only once for all parser threads when the last thread exits.
        """
        pass
