from connarchitecture.exceptions import FatalException
from connarchitecture.constants import Constants
from connarchitecture.server import ThreadedServer
from connarchitecture.decorators import overrides
from connarchitecture.queue import ConnectorQueue
from connarchitecture.models import Transaction
from connarchitecture.dir_watcher import DirWatcher
import json
import configparser
from queue import Empty
import multiprocessing
import time
from abc import ABC
from datetime import datetime, timezone
from threading import Thread
import os


class ConnectorStatistics(ABC):
    polled_count = 0
    parsed_count = 0
    completed_count = 0
    error_count = 0
    last_error = None

    @staticmethod
    def get_statistics():
        stats = ""
        stats += f"{'Polled':>15}: {ConnectorStatistics.polled_count}\n"
        stats += f"{'Parsed':>15}: {ConnectorStatistics.parsed_count}\n"
        stats += f"{'Completed':>15}: {ConnectorStatistics.completed_count}\n"
        stats += f"{'Errors':>15}: {ConnectorStatistics.error_count}\n"
        if ConnectorStatistics.last_error is not None:
            stats += f"{'Last error':>15}: {ConnectorStatistics.last_error}"
        return stats


class Connector(ThreadedServer):
    CMD_STOP = ('stop', 'stop connector')
    CMD_STATS = ('stats', 'show statistics')
    CMD_HEAD = ('head', 'show the first items in the queues')
    COMMANDS = [CMD_STOP, CMD_STATS, CMD_HEAD]

    def __init__(self, config_path):
        self._load_config(config_path)
        ThreadedServer.__init__(self, self._name, self._host, self._port)
        self._poller_in_queue = ConnectorQueue()
        self._poller_out_queue = ConnectorQueue()
        self._parser_out_queue = ConnectorQueue()
        self._pollers = []
        self._parsers = []
        self._senders = []
        self._event_queue = ConnectorQueue()
        self.set_event_queue(self._event_queue)
        self._transaction_handler = None
        if self._transaction_handler_class:
            self._transaction_queue = ConnectorQueue()
        else:
            self._transaction_queue = None
        self._continue = False

    def _load_class(self, cls):
        parts = cls.split('.')
        module = ".".join(parts[:-1])
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m

    def _load_config(self, config_path):
        config = configparser.ConfigParser()
        config.read(config_path)
        self._name = config.get(Constants.CONFIG_SECTION_CONNECTOR, Constants.CONFIG_CONNECTOR_NAME)
        self._host = config.get(Constants.CONFIG_SECTION_CONNECTOR, Constants.CONFIG_CONNECTOR_HOST)
        self._port = int(config.get(Constants.CONFIG_SECTION_CONNECTOR, Constants.CONFIG_CONNECTOR_PORT))

        self._directory_watcher_directory = config.get(Constants.CONFIG_SECTION_CDIRECTORY_WATCHER, Constants.CONFIG_DIRECTORY_WATCHER_DIRECTORY)

        self._poller_classes_config = {}
        for section in config.sections():
            if section.startswith(Constants.CONFIG_SECTION_POLLER):
                self._poller_classes_config[section] = {
                    'class': self._load_class(config.get(section, Constants.CONFIG_POLLER_CLASS)),
                    'topic': config.get(section, Constants.CONFIG_POLLER_TOPIC),
                    'min_poller_threads': int(config.get(section, Constants.CONFIG_POLLER_MIN_THREADS)),
                    'max_poller_threads': int(config.get(section, Constants.CONFIG_POLLER_MAX_THREADS)),
                    'poller_args': json.loads(config.get(section, Constants.CONFIG_POLLER_ARGS))
                }

        self._parser_class = self._load_class(config.get(Constants.CONFIG_SECTION_PARSER, Constants.CONFIG_PARSER_CLASS))
        self._min_parser_threads = int(config.get(Constants.CONFIG_SECTION_PARSER, Constants.CONFIG_PARSER_MIN_THREADS))
        self._max_parser_threads = int(config.get(Constants.CONFIG_SECTION_PARSER, Constants.CONFIG_PARSER_MAX_THREADS))
        self._parser_args = json.loads(config.get(Constants.CONFIG_SECTION_PARSER, Constants.CONFIG_PARSER_ARGS))

        self._sender_class = self._load_class(config.get(Constants.CONFIG_SECTION_SENDER, Constants.CONFIG_SENDER_CLASS))
        self._min_sender_threads = int(config.get(Constants.CONFIG_SECTION_SENDER, Constants.CONFIG_SENDER_MIN_THREADS))
        self._max_sender_threads = int(config.get(Constants.CONFIG_SECTION_SENDER, Constants.CONFIG_SENDER_MAX_THREADS))
        self._sender_args = json.loads(config.get(Constants.CONFIG_SECTION_SENDER, Constants.CONFIG_SENDER_ARGS))

        if config.has_section(Constants.CONFIG_SECTION_TRANSACTION_HANDLER):
            self._transaction_handler_class = self._load_class(config.get(Constants.CONFIG_SECTION_TRANSACTION_HANDLER, Constants.CONFIG_TRANSACTION_HANDLER_CLASS))
            self._transaction_handler_args = json.loads(config.get(Constants.CONFIG_SECTION_TRANSACTION_HANDLER, Constants.CONFIG_TRANSACTION_HANDLER_ARGS))
        else:
            self._transaction_handler_class = None

    def _get_num_of_cpu_threads(self):
        threads = 1
        cpus = multiprocessing.cpu_count()
        return max(threads, cpus)

    def _setup_pollers(self):
        threads = []
        cpus = self._get_num_of_cpu_threads()

        for key in self._poller_classes_config:
            poller_class = self._poller_classes_config[key]['class']
            num_of_threads = max(self._poller_classes_config[key]['min_poller_threads'], min(self._poller_classes_config[key]['max_poller_threads'], cpus))
            name_prefix = poller_class.__name__
            for i in range(num_of_threads):
                t = poller_class(f"{name_prefix}-{i+1}", **self._poller_classes_config[key]['poller_args'])
                t.set_in_queue(self._poller_in_queue)
                t.set_out_queue(self._poller_out_queue)
                t.set_event_queue(self._event_queue)
                t.set_topic(self._poller_classes_config[key]['topic'])
                threads.append(t)
        return threads

    def _setup_parsers(self):
        threads = []
        cpus = self._get_num_of_cpu_threads()
        num_of_threads = max(self._min_parser_threads, min(self._max_parser_threads, cpus))
        name_prefix = self._parser_class.__name__
        for i in range(num_of_threads):
            t = self._parser_class(f"{name_prefix}-{i+1}", **self._parser_args)
            t.set_in_queue(self._poller_out_queue)
            t.set_out_queue(self._parser_out_queue)
            t.set_event_queue(self._event_queue)
            threads.append(t)
        return threads

    def _setup_senders(self):
        threads = []
        cpus = self._get_num_of_cpu_threads()
        num_of_threads = max(self._min_sender_threads, min(self._max_sender_threads, cpus))
        name_prefix = self._sender_class.__name__
        for i in range(num_of_threads):
            t = self._sender_class(f"{name_prefix}-{i+1}", **self._sender_args)
            t.set_in_queue(self._parser_out_queue)
            t.set_event_queue(self._event_queue)
            threads.append(t)
        return threads

    def _setup_transaction_handler(self):
        handler = self._transaction_handler_class(**self._transaction_handler_args)
        handler.set_transaction_queue(self._transaction_queue)
        return handler

    def _setup_workers(self):
        pollers = self._setup_pollers()
        parsers = self._setup_parsers()
        senders = self._setup_senders()
        return (pollers, parsers, senders)

    def _start_workers(self, threads):
        for t in threads:
            t.start()

    def _join_workers(self, threads):
        for t in threads:
            if t.is_alive():
                t.join()

    def _start_dir_watcher(self, dir, queue):
        if not os.path.exists(dir):
            os.makedirs(dir)
        dir_watcher = DirWatcher(dir_to_watch=dir, queue=queue)
        dir_watcher_thread = Thread(target=dir_watcher.run, args=())
        dir_watcher_thread.daemon = True
        dir_watcher_thread.start()

    def _stop_workers(self, threads):
        for t in threads:
            t.stop()

    def _stop(self):
        self._stop_workers(self._pollers)
        self._stop_workers(self._parsers)
        self._stop_workers(self._senders)
        if self._transaction_handler:
            self._stop_workers([self._transaction_handler])

        self.stop_server()

        self._join_workers(self._pollers)
        self._join_workers(self._parsers)
        self._join_workers(self._senders)

        if self._transaction_handler:
            self._join_workers([self._transaction_handler])

    def _kill(self):
        self.disconnect()
        self._stop()

    def _update_statistics(self, event):
        if event.exception or event.type == Constants.EVENT_ERROR:
            ConnectorStatistics.error_count += 1
        elif event.type == Constants.EVENT_POLLED:
            ConnectorStatistics.polled_count += 1
        elif event.type == Constants.EVENT_PARSED:
            ConnectorStatistics.parsed_count += 1
        elif event.type == Constants.EVENT_SEND:
            ConnectorStatistics.completed_count += 1

    def _head(self):
        result = ""
        result += f"Poller Queue: {self._poller_out_queue.head()}\n"
        result += f"Parser Queue: {self._parser_out_queue.head()}"
        return result

    def _commit(self, poll_reference, success):
        if self._transaction_queue:
            self._transaction_queue.put(Transaction(poll_reference, success))

    def connect(self):
        utc_dt = datetime.now(timezone.utc)  # UTC time
        self._start_time = utc_dt.astimezone()  # local time

        if self._transaction_handler_class:
            self._transaction_handler = self._setup_transaction_handler()

        self._pollers, self._parsers, self._senders = self._setup_workers()

        self._start_workers(self._pollers)
        self._start_workers(self._parsers)
        self._start_workers(self._senders)

        if self._transaction_handler:
            self._start_workers([self._transaction_handler])

        self._start_dir_watcher(dir=self._directory_watcher_directory, queue=self._poller_in_queue)

        self.start()

        self._continue = True
        while self._continue:
            try:
                event = self._event_queue.get(block=False)
            except Empty:
                pass
            else:
                self._update_statistics(event)

                if not event.exception and (event.type == Constants.EVENT_SEND or event.type == Constants.EVENT_DONE):
                    self._commit(event.get_poll_reference(), success=True)

                if event.exception or event.type == Constants.EVENT_ERROR:
                    self.log_exception(exception=event.exception)

                    self._commit(event.get_poll_reference(), success=False)
                    ConnectorStatistics.last_error = event.exception
                    if event.exception:
                        self.log_error(event.exception)
                        if isinstance(event.exception, FatalException):
                            self._kill()
            time.sleep(1)

    def disconnect(self):
        self._continue = False

    @overrides(ThreadedServer)
    def get_hello_message(self):
        hello = "=============================================================\n"
        hello += f"{self.get_name()}\n"
        hello += "-------------------------------------------------------------\n"
        hello += f"{'Started':>10}: {self._start_time}\n"
        hello += f"{'Poller':>10}: {self._poller_class.__name__} ({len(self._pollers)})\n"
        hello += f"{'Parser':>10}: {self._parser_class.__name__} ({len(self._parsers)})\n"
        hello += f"{'Sender':>10}: {self._sender_class.__name__} ({len(self._senders)})\n"
        hello += "-------------------------------------------------------------\n"
        hello += "Connector commands:\n"
        hello += "-------------------------------------------------------------\n"
        for cmd in Connector.COMMANDS:
            hello += f"{cmd[0]:>10}: {cmd[1]}\n"
        hello += "=============================================================\n"
        return hello

    @overrides(ThreadedServer)
    def event_received(self, server_event):
        cmd = server_event.message
        self.log(f"received server event {server_event}")
        if cmd == Connector.CMD_STATS[0]:
            stats = ConnectorStatistics.get_statistics()
            self.send_msg(server_event.client_connection, stats)
        elif cmd == Connector.CMD_HEAD[0]:
            head = self._head()
            self.send_msg(server_event.client_connection, head)
        elif cmd == Connector.CMD_STOP[0]:
            self._kill()
        else:
            self.send_msg(server_event.client_connection, "Unknown command")
