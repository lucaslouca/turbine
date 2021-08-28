from connarchitecture.logging_component import LoggingComponent
from implementation.data_extraction_request import DataExtractionRequest
from implementation.extractor_result import ExtractorResult
from implementation.exceptions import ExtractorException
import os
from abc import ABC, abstractmethod


class AbstractExtractor(ABC, LoggingComponent):
    def __init__(self):
        self._name_str = None
        LoggingComponent.__init__(self, self.component_name())

    def do_extract(self, file: DataExtractionRequest):
        result = ExtractorResult()
        result.extractor_name = self.component_name()
        try:
            result.result_list = self.extract(file)
        except Exception as e:
            self.log_exception(message="Something went wrong during exctract", exception=e)
        return result

    @abstractmethod
    def extract(self, file: DataExtractionRequest):
        pass

    def does_support_input(self, file: DataExtractionRequest):
        result = False
        try:
            result = self.supports_input(file)
        except Exception as e:
            self.log_exception(message="Something went wrong during supports_input", exception=e)
        return result

    @abstractmethod
    def supports_input(self, file: DataExtractionRequest):
        return False

    def component_name(self):
        if self._name_str:
            return self._name_str
        else:
            return os.path.splitext(os.path.basename(__file__))[0]
