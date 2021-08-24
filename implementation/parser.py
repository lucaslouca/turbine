from connarchitecture.abstract_parser import AbstractParser
from connarchitecture.decorators import overrides
from implementation.abstract_extractor import AbstractExtractor
from implementation.model.data_extraction_request import DataExtractionRequest
from implementation.extractor_result import ExtractorResult
import os
import copy


class FileParser(AbstractParser):
    def __init__(self, name, **kwargs):
        AbstractParser.__init__(self, name, **kwargs)
        self._extractors = []
        self._extractors_default = []

    def _load_class(self, cls):
        parts = cls.split('.')
        module = ".".join(parts[:-1])
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m

    def _load_extractors(self):
        self.log("Loading extractors")
        result = []
        for f in sorted(os.listdir("extractors")):
            path = os.path.join("extractors", f)
            if os.path.isfile(path) and path.endswith('.py'):
                name = os.path.splitext(os.path.basename(f))[0]
                mod_name = f"extractors.{name}.{name}"
                extractor_class = self._load_class(mod_name)

                if issubclass(extractor_class, AbstractExtractor):
                    extractor = extractor_class()
                    extractor._name_str = name
                    result.append(extractor)
                else:
                    self.log(f"{name} does not appear to be an extractor type. Ignoring")

        return result

    @overrides(AbstractParser)
    def static_initialize(self):
        pass

    @overrides(AbstractParser)
    def initialize(self):
        self.log("init")
        self._extractors = self._load_extractors()
        self.log(f"Loaded {len(self._extractors)} extractor(s): {[ex.component_name() for ex in self._extractors]}")

    @overrides(AbstractParser)
    def parse(self, file: DataExtractionRequest, poll_reference=None):
        result = []
        do_extractors = [extractor for extractor in self._extractors if extractor.supports_input(file)]
        if not do_extractors:
            do_extractors = self._extractors_default

        for extractor in do_extractors:
            if extractor.does_support_input(file):
                extractor_result = extractor.do_extract(file)
                result.append(extractor_result)

        if not result:
            self.log(f"Nothing extracted while parsing {poll_reference}")

        return result

    @overrides(AbstractParser)
    def cleanup(self):
        self.log("cleanup")

    @overrides(AbstractParser)
    def static_cleanup(self):
        self.log("static cleanup")
