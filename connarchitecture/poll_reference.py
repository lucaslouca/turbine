class PollReference:

    def __init__(self):
        self.file = None
        self.ticker = None
        self.url = None
        self.year = None
        self.concept = None
        self.poller = None

    def __repr__(self):
        return f'<PollReference(file={self.file}, ticker={self.ticker}, url={self.url}, concept={self.concept}, year={self.year}, poller={self.poller})>'
