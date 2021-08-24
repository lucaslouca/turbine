from abc import ABC


class ServerEvent:
    def __init__(self, message, client_connection, client_name):
        self.message = message
        self.client_connection = client_connection
        self.client_name = client_name

    def __str__(self):
        return f"{self.client_name} - {self.message}"


class ConnectorEvent:
    def __init__(self, type, exception=None, poll_reference=None):
        self.exception = exception
        self.type = type
        self._poll_reference = poll_reference

    def get_poll_reference(self):
        return self._poll_reference

    def __str__(self):
        return f"Type: {self.type} - {self.exception if self.exception else ''}"


class AbstractResult(ABC):
    def __init__(self, result, poll_reference=None):
        self._result = result
        self._poll_reference = poll_reference

    def get_name(self):
        return type(self).__name__

    def get_poll_reference(self):
        return self._poll_reference

    def get_result(self):
        return self._result

    def __str__(self):
        if self._result and isinstance(self._result, list):
            cut = 10
            cutted_result = self._result[:cut]
            if len(self._result) > cut:
                cutted_result += "..."
            return f"{self.get_name()}: '{cutted_result}'"
        else:
            return f"{self.get_name()}: '{self._result}'"


class PollerResult(AbstractResult):
    pass


class ParserResult(AbstractResult):
    pass

class Transaction:
    def __init__(self, object, success):
        self.object = object
        self.success = success