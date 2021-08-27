from implementation.abstract_extractor import AbstractExtractor
from implementation.model.data_extraction_request import DataExtractionRequest
from connarchitecture.decorators import overrides
from implementation.model.price import Price
from implementation.model.ticker import Ticker
import datetime as dt
from datetime import date
import csv


class PriceExtractor(AbstractExtractor):
    @overrides(AbstractExtractor)
    def supports_input(self, request: DataExtractionRequest):
        return request.file_extension and request.file_extension.upper() == ".CSV"

    def _read_rows_from_file(self, file: str) -> list[dict]:
        result = []
        try:
            with open(file, newline='') as csvfile:
                csvreader = csv.DictReader(csvfile)
                for row in csvreader:
                    result.append(row)
        except Exception as e:
            self.log_exception(exception=e)
        return result

    @ overrides(AbstractExtractor)
    def extract(self, request: DataExtractionRequest):
        self.log(f"Proccessing '{request.file}'")
        result = []
        rows = self._read_rows_from_file(file=request.file)
        for row in rows:
            try:
                result.append(
                    Price(
                        ticker=Ticker(symbol=request.ticker),
                        date=dt.datetime.strptime(row['Date'], "%Y-%m-%d").date(),
                        open=float(row['Open']),
                        high=float(row['High']),
                        low=float(row['Low']),
                        close=float(row['Close']),
                        adj_close=float(row['Adj Close']),
                        volume=float(row['Volume'])
                    )
                )
            except Exception as e:
                self.log_exception(exception=e)

        return result