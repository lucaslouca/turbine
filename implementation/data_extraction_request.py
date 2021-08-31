import os


class DataExtractionRequest:
    def __init__(self, poll_reference, ticker: str, data: dict):
        self.poll_reference = poll_reference
        self.ticker = ticker
        self.data = data

    @property
    def file_extension(self):
        _, file_extension = os.path.splitext(self.poll_reference.file)
        return file_extension.upper() if file_extension else 'UNKNOWN_EXTENSION'

    @property
    def file_name(self):
        filename, _ = os.path.splitext(self.poll_reference.file)
        return filename

    def __repr__(self):
        return f'<DataExtractionRequest(poll_reference={self.poll_reference}, ticker={self.ticker}, data={self.data})>'
