import os


class DataExtractionRequest:
    def __init__(self, url: str, file: str, ticker: str, concept: str, year: int):
        self.url = url
        self.file = file
        self.ticker = ticker
        self.concept = concept
        self.year = year

    @property
    def file_extension(self):
        _, file_extension = os.path.splitext(self.file)
        return file_extension.upper() if file_extension else 'UNKNOWN_EXTENSION'

    @property
    def file_name(self):
        filename, _ = os.path.splitext(self.file)
        return filename

    def __hash__(self):
        return hash(self.url)

    def __eq__(self, other):
        return isinstance(other, DataExtractionRequest) and self.url == other.url

    def __str__(self):
        return f"{self.file}"

    def __repr__(self):
        return f'<DataExtractionRequest(url={self.url}, file={self.file}, ticker={self.ticker}, concept={self.concept}, year={self.year})>'
