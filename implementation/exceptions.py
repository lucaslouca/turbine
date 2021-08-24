from connarchitecture.exceptions import ProcessException


class ExtractorException(ProcessException):
    def __init__(self, message, poll_reference=None):
        ProcessException.__init__(self, message, poll_reference)
