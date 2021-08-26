from connarchitecture.abstract_poller import AbstractPoller
from connarchitecture.decorators import overrides
from connarchitecture.queue import ConnectorQueue
from implementation.model.data_extraction_request import DataExtractionRequest
from implementation.file_dir_watcher import FileDirWatcher
import os
from pathlib import Path
from threading import Thread
from queue import Empty
import requests


class PricePoller(AbstractPoller):
    _shared_cik_to_ticker_map = {}
    _shared_input_queue = ConnectorQueue()

    def __init__(self, name, **kwargs):
        AbstractPoller.__init__(self, name)
        self._file_dir = kwargs['file_dir']
        self._cache_dir = 'cache/prices'
        if not os.path.exists(self._file_dir):
            os.makedirs(self._file_dir)

    def _spawn_dir_watcher(self, dir):
        dir_watcher = FileDirWatcher(dir, PricePoller._shared_input_queue)
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
    def poll(self):
        extraction_request = None
        file = None
        success = False
        try:
            ticker, concept, year = PricePoller._shared_input_queue.get_topic(topic=self._topic, block=False)
            success = True
        except Empty:
            pass
        except Exception as e:
            self.log_error(e)
        finally:
            return (extraction_request, file, success)

    @ overrides(AbstractPoller)
    def cleanup(self):
        self.log("cleanup")

    @ overrides(AbstractPoller)
    def static_cleanup(self):
        self.log("static cleanup")
