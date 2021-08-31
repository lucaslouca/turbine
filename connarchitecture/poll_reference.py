class PollReference:

    def __init__(self):
        self.file = None
        self.ticker = None
        self.url = None
        self.year = None
        self.concept = None
        self.poller = None

    def __repr__(self):
        f = [f"file={self.file}" if self.file else None,
             f"ticker={self.ticker}" if self.ticker else None,
             f"url={self.url}" if self.url else None,
             f"concept={self.concept}" if self.concept else None,
             f"year={self.year}" if self.year else None,
             f"poller={self.poller}" if self.poller else None
             ]
        fields = ", ".join([x for x in f if x is not None])

        return f'<PollReference({fields})>'
