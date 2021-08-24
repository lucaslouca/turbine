import os


class GenericException(Exception):
    def __init__(self, message=None):
        self._message = message
        super().__init__(self._message)

    def __str__(self):
        return f"{self._message}"


class ConnectorException(GenericException):
    def __init__(self, message=None):
        GenericException.__init__(self, message)


class ProcessException(GenericException):
    def __init__(self, message=None, poll_reference=None):
        GenericException.__init__(self, message=message)
        self._poll_reference = poll_reference

    def get_poll_reference(self):
        return self._poll_reference

    def __str__(self):
        prefix = super().__str__()
        return f"{prefix}{'(poll_reference={})'.format(self._poll_reference) if self._poll_reference else ''}"


class FatalException(ProcessException):
    def __init__(self, message=None, poll_reference=None):
        ProcessException.__init__(self, message=message, poll_reference=poll_reference)


class PollerException(ProcessException):
    def __init__(self, message=None, poll_reference=None):
        ProcessException.__init__(self, message=message, poll_reference=poll_reference)


class ParserException(ProcessException):
    def __init__(self, message=None, poll_reference=None):
        ProcessException.__init__(self, message=message, poll_reference=poll_reference)


class SenderException(ProcessException):
    def __init__(self, message=None, poll_reference=None):
        ProcessException.__init__(self, message=message, poll_reference=poll_reference)
