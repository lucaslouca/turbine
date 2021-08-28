class ExtractorResult:
    def __init__(self):
        self.result_list = None
        self.extractor_name = None

    def __repr__(self):
        return f'<ExtractorResult(extractor_name={self.extractor_name}, result_list={self.result_list[:min(2, len(self.result_list))]}...)>'
