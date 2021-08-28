import os


class DataExtractionRequest:
    def __init__(self, file: str, ticker: str, data: dict):
        self.file = file
        self.ticker = ticker
        self.data = data

    @property
    def file_extension(self):
        _, file_extension = os.path.splitext(self.file)
        return file_extension.upper() if file_extension else 'UNKNOWN_EXTENSION'

    @property
    def file_name(self):
        filename, _ = os.path.splitext(self.file)
        return filename

    def __str__(self):
        return f"{self.file}"

    def __repr__(self):
        return f'<DataExtractionRequest(file={self.file}, ticker={self.ticker}, data={self.data})>'
