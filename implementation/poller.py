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


class Poller(AbstractPoller):
    _shared_cik_to_ticker_map = {}
    _shared_input_queue = ConnectorQueue()

    def __init__(self, name, **kwargs):
        AbstractPoller.__init__(self, name)
        self._file_dir = kwargs['file_dir']
        if not os.path.exists(self._file_dir):
            os.makedirs(self._file_dir)

    def _spawn_dir_watcher(self, dir):
        dir_watcher = FileDirWatcher(dir, Poller._shared_input_queue)
        dir_watcher_thread = Thread(target=dir_watcher.run, args=())
        dir_watcher_thread.daemon = True
        dir_watcher_thread.start()

    def _download_tickers(self, file: str) -> str:
        self.log('Downloading CIK <-> tickers mapping')

        result = False
        r = requests.get('https://www.sec.gov/include/ticker.txt', stream=True)
        if r.status_code == requests.codes.ok:
            with open(file, 'wb') as f:
                for data in r:
                    f.write(data)
                result = True
        return result

    def _read_tickers_file(self, file: str) -> dict[str, str]:
        result = {}
        with open(file) as f:
            for line in f:
                (ticker, cik) = line.split()
                result[ticker.strip().lower()] = '0' * (10 - len(cik.strip())) + cik.strip()
        return result

    def _fetch_cik_to_ticker_map(self, file: str) -> dict[str, str]:
        if not os.path.exists(file):
            if not self._download_tickers(file):
                raise Exception('Could not download ticker file!')
        return self._read_tickers_file(file)

    def _generate_url_for_ticker(self, ticker: str, ciks_to_tickers: dict[str, str], concept: str) -> tuple[str, str, str]:
        result = None

        if ticker.lower() in ciks_to_tickers:
            cik = ciks_to_tickers[ticker.lower()]
            result = (ticker, concept, f'https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/{concept}.json')

        return result

    def _download_concept(self, url: str, ticker: str, concept: str, destination_root_dir: str) -> str:
        folder = f'{destination_root_dir}/{ticker}'
        file = f'{folder}/{concept}.json'

        if os.path.exists(file):
            self.log(f'Using cached {concept} ({file})')
            return file
        else:
            os.makedirs(folder, exist_ok=True)

            self.log(f'Downloading {concept} for {ticker}')
            r = requests.get(url, stream=True)
            if r.status_code == requests.codes.ok:
                with open(file, 'wb') as f:
                    for data in r:
                        f.write(data)
                    return file
            return None

    @overrides(AbstractPoller)
    def static_initialize(self):
        os.makedirs('cache', exist_ok=True)
        self._spawn_dir_watcher(self._file_dir)
        Poller._shared_cik_to_ticker_map = self._fetch_cik_to_ticker_map(file='cache/ticker.txt')

    @overrides(AbstractPoller)
    def initialize(self):
        self.log("init")

    @overrides(AbstractPoller)
    def poll(self):
        extraction_request = None
        file = None
        success = False
        try:
            ticker, concept, year = Poller._shared_input_queue.get(block=False)
            ticker, concept, url = self._generate_url_for_ticker(ticker=ticker, ciks_to_tickers=Poller._shared_cik_to_ticker_map, concept=concept)
            file = self._download_concept(url=url, ticker=ticker, concept=concept, destination_root_dir='cache')
            extraction_request = DataExtractionRequest(url=url, file=file, ticker=ticker, concept=concept, year=year)
            self.log(f"Polled '{file}'")
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
