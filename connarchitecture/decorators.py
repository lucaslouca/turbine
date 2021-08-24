from connarchitecture.exceptions import PollerException
from functools import wraps


def overrides(interface_class):
    @wraps(interface_class)
    def overrider(method):
        assert(method.__name__ in dir(interface_class))
        return method
    return overrider


def try_poller(func):
    @wraps(func)
    def silenceit(self):
        try:
            func(self, *args, **kwargs)
        except PollerException as e:
            self.log_error(e)
        return silenceit
