from connarchitecture.constants import Constants
from connarchitecture.models import ConnectorEvent
import logging
from logging.config import fileConfig
from logging.handlers import TimedRotatingFileHandler, WatchedFileHandler
import os


class ConnectorFileHandler(TimedRotatingFileHandler, WatchedFileHandler):
    def __init__(self, file_name, when='M', interval=10, backup_count=5):
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        TimedRotatingFileHandler.__init__(self, filename=file_name, when=when, interval=interval, backupCount=backup_count, encoding='utf8')
        self.dev, self.ino = -1, -1

    def emit(self, record):
        self.reopenIfNeeded()
        super().emit(record)


class LoggingComponent:
    LOGGING_OK = 'OK'
    LOGGING_WARNING = 'WARNING'
    LOGGING_ERROR = 'ERR'
    _logger = None
    _logger_warning = None
    _logger_error = None

    def __init__(self, name):
        self._name_str = name
        fileConfig(Constants.LOGGING_CONFIG)
        LoggingComponent._logger = logging.getLogger()
        LoggingComponent._logger_warning = logging.getLogger('warning_logger')
        LoggingComponent._logger_error = logging.getLogger('error_logger')

    def component_name(self):
        return None

    def get_name(self):
        overriden_name = self.component_name()
        if overriden_name:
            return overriden_name
        elif self._name_str:
            return self._name_str
        else:
            return os.path.splitext(os.path.basename(__file__))[0]

    def _unwrap_exception(self, exception: Exception):
        _KEY_LEVEL = "LEVEL"
        _KEY_EXC_NAME = "EXCEPTION"
        _KEY_FILE_NAME = "FILE"
        _KEY_LINE_NO = "LINE"
        _KEY_MESSAGE = "MESSAGE"

        def _unwrap(e):
            result = []
            cause = e
            level = 0
            while cause:
                tb = cause.__traceback__
                while tb:
                    file_name = tb.tb_frame.f_code.co_filename
                    line_no = str(tb.tb_lineno)
                    exc_name = type(cause).__name__
                    result.append({
                        _KEY_LEVEL: level,
                        _KEY_EXC_NAME: exc_name,
                        _KEY_FILE_NAME: file_name,
                        _KEY_LINE_NO: line_no,
                        _KEY_MESSAGE: str(cause)
                    })
                    tb = tb.tb_next
                    level += 1
                cause = cause.__cause__
            return result

        def _pretty_string(lines):
            if not lines:
                return ""

            PAD = 4
            max_level = max(lines, key=lambda x: x[_KEY_LEVEL] + len(x[_KEY_EXC_NAME]))
            widths = [
                max_level[_KEY_LEVEL] + len(max_level[_KEY_EXC_NAME]) + PAD,
                len(max(lines, key=lambda x: len(x[_KEY_FILE_NAME]))[_KEY_FILE_NAME]) + PAD,
                len(max(lines, key=lambda x: len(x[_KEY_LINE_NO]))[_KEY_LINE_NO]) + PAD,
                len(max(lines, key=lambda x: len(x[_KEY_MESSAGE]))[_KEY_MESSAGE]) + PAD
            ]

            widths_dict = {f"w{index}": value for index, value in enumerate(widths, 1)}
            line = "{:<{w1}} {:<{w2}} {:<{w3}} {:<{w4}}\n"
            header_fields = [_KEY_EXC_NAME, _KEY_FILE_NAME, _KEY_LINE_NO, _KEY_MESSAGE]
            result = line.format(*header_fields, **widths_dict)
            for d in lines:
                fields = [' '*d[_KEY_LEVEL]+d[_KEY_EXC_NAME], d[_KEY_FILE_NAME], d[_KEY_LINE_NO], d[_KEY_MESSAGE]]
                result += line.format(*fields, **widths_dict)

            return result

        lines = _unwrap(exception)
        return _pretty_string(lines)

    def _log(self, message, status, descriptor=None):
        descriptor = descriptor if descriptor else status

        name = self.get_name()
        if len(name) > 30:
            name = name[:27] + "..."
        out = (f"[{name:>30}][{descriptor}][{message}]")
        if status == LoggingComponent.LOGGING_ERROR:
            LoggingComponent._logger_error.error(out)
        elif status == LoggingComponent.LOGGING_WARNING:
            LoggingComponent._logger_warning.warning(out)
        else:
            LoggingComponent._logger.info(out)

    def log(self, message, descriptor=None):
        self._log(message=message, status=LoggingComponent.LOGGING_OK, descriptor=descriptor)

    def log_exception(self, exception: Exception, message=None, descriptor=None):
        message = f"{'{}:'.format(message) if message else ''}\n{self._unwrap_exception(exception)}"
        self.log_error(message=message, descriptor=descriptor)

    def log_warning(self, message, descriptor=None):
        self._log(message=message, status=LoggingComponent.LOGGING_WARNING, descriptor=descriptor)

    def log_error(self, message, descriptor=None):
        self._log(message=message, status=LoggingComponent.LOGGING_ERROR, descriptor=descriptor)

    def set_event_queue(self, event_queue):
        self._event_queue = event_queue

    def throw(self, exception, poll_reference=None):
        event = ConnectorEvent(type=Constants.EVENT_ERROR, exception=exception, poll_reference=poll_reference)
        self._event_queue.put(event)

    def update(self, type, poll_reference):
        event = ConnectorEvent(type=type, poll_reference=poll_reference)
        self._event_queue.put(event)
