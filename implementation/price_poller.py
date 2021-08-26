from connarchitecture.abstract_poller import AbstractPoller
from connarchitecture.decorators import overrides
from connarchitecture.queue import ConnectorQueue
from implementation.model.data_extraction_request import DataExtractionRequest
from implementation.file_dir_watcher import FileDirWatcher
import os
from pathlib import Path
from threading import Thread
import requests


class PricePoller(AbstractPoller):
    _shared_cik_to_ticker_map = {}

    def __init__(self, name, **kwargs):
        AbstractPoller.__init__(self, name)
        self._file_dir = 'in'
        self._cache_dir = 'cache/prices'
        if not os.path.exists(self._file_dir):
            os.makedirs(self._file_dir)

    def _spawn_dir_watcher(self, dir):
        dir_watcher = FileDirWatcher(dir, self._in_queue)
        dir_watcher_thread = Thread(target=dir_watcher.run, args=())
        dir_watcher_thread.daemon = True
        dir_watcher_thread.start()

    @overrides(AbstractPoller)
    def static_initialize(self):
        self.log('static init')
        self._spawn_dir_watcher(self._file_dir)

    @overrides(AbstractPoller)
    def initialize(self):
        self.log("init")
        os.makedirs(self._cache_dir, exist_ok=True)

    @overrides(AbstractPoller)
    def get_topic(self):
        return self._topic

    @overrides(AbstractPoller)
    def poll(self, items):
        extraction_request = None
        file = None
        success = False

        ticker, years = items
        self.log(f"Polling {ticker} historical data for years: {years}")
        success = True
        return (extraction_request, file, success)

    @ overrides(AbstractPoller)
    def cleanup(self):
        self.log("cleanup")

    @ overrides(AbstractPoller)
    def static_cleanup(self):
        self.log("static cleanup")
