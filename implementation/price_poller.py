from connarchitecture.abstract_poller import AbstractPoller
from connarchitecture.decorators import overrides
from implementation.model.data_extraction_request import DataExtractionRequest
import os
from datetime import datetime
import requests
import re


class PricePoller(AbstractPoller):
    _shared_cik_to_ticker_map = {}

    def __init__(self, name, **kwargs):
        AbstractPoller.__init__(self, name)
        self._cache_dir = 'cache/prices'

    @overrides(AbstractPoller)
    def static_initialize(self):
        self.log('static init')

    @overrides(AbstractPoller)
    def initialize(self):
        self.log("init")
        os.makedirs(self._cache_dir, exist_ok=True)

    @overrides(AbstractPoller)
    def get_topic(self):
        return self._topic

    def _epoch(self, year: int, month: int, day: int) -> int:
        return int((datetime(year, month, day, 0, 0) - datetime(1970, 1, 1)).total_seconds())

    def _download_historical_data(self, ticker: str, fy: int, destination_root_dir: str) -> str:
        folder = f'{destination_root_dir}/{fy}'
        file = f'{folder}/{ticker}.csv'

        start = int(self._epoch(year=fy, month=1, day=1))
        end = int(self._epoch(year=fy, month=12, day=31))

        if os.path.exists(file):
            self.log(f'Using cached historical data (file={file}) for {ticker}')
            return file
        else:
            os.makedirs(folder, exist_ok=True)

            self.log(f'Downloading historical data for {ticker}')

            crumble_link = f'https://finance.yahoo.com/quote/{ticker}/history?p={ticker}'
            crumble_regex = r'CrumbStore":{"crumb":"(.*?)"}'
            cookie_regex = r'set-cookie: (.*?);'
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}

            session = requests.Session()
            response = session.get(crumble_link, headers=headers)

            # get crumbs
            text = str(response.content)
            match = re.search(crumble_regex, text)
            crumbs = match.group(1)
            url = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={start}&period2={end}&interval=1d&events=history&crumb={crumbs}'

            # get cookie
            cookie = session.cookies.get_dict()
            r = requests.get(url, cookies=cookie, timeout=5, stream=True, headers=headers)
            data = r.text
            with open(file, 'w') as f:
                f.write(data)
                return file

    @ overrides(AbstractPoller)
    def poll(self, items):
        extraction_request = None
        file = None
        success = False

        ticker, year = items
        self.log(f"Polling {ticker} historical data for year: {year}")

        file = self._download_historical_data(ticker=ticker, fy=year, destination_root_dir=self._cache_dir)
        extraction_request = DataExtractionRequest(file=file, ticker=ticker, data={'year': year})

        success = True
        return (extraction_request, file, success)

    @ overrides(AbstractPoller)
    def cleanup(self):
        self.log("cleanup")

    @ overrides(AbstractPoller)
    def static_cleanup(self):
        self.log("static cleanup")
