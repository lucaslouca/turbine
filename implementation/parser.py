from connarchitecture.abstract_parser import AbstractParser
from connarchitecture.decorators import overrides
from implementation.abstract_extractor import AbstractExtractor
from implementation.data_extraction_request import DataExtractionRequest
from implementation.extractor_result import ExtractorResult
import os


class Parser(AbstractParser):
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
        self.log(f"Loaded {len(self._extractors)} extractor(s):\n{self._loaded_extractors_message(self._extractors)}\n")

    def _loaded_extractors_message(self, extractors) -> str:
        result = []
        PAD = 4
        COLUMNS = 3
        extractor_names = sorted([ex.component_name() for ex in extractors])
        max_name_len = max([len(name) for name in extractor_names])
        max_number_len = len(str(len(extractors)+1))
        column_width = max_name_len + max_number_len + PAD
        no_of_lines = len(extractors)//COLUMNS + 1

        start = 0
        end = 0
        line = "{:<{w}}"*COLUMNS + '\n'
        for l in range(1, no_of_lines):
            end = start + l*COLUMNS
            fields = [f"{start+index+1}. {name}" for index, name in enumerate(extractor_names[start:end])]
            result.append(line.format(*fields, **{'w': column_width}))
            start = end

        # Append any remaining extractors
        if end < len(extractors):
            line = "{:<{w}}"*(len(extractors) - end)
            fields = [f"{end+index+1}. {name}" for index, name in enumerate(extractor_names[end:])]
            result.append(line.format(*fields, **{'w': column_width}))

        return '\n'.join(result)

    @overrides(AbstractParser)
    def parse(self, request: DataExtractionRequest, poll_reference=None):
        result = []
        do_extractors = [extractor for extractor in self._extractors if extractor.supports_input(request)]
        if not do_extractors:
            do_extractors = self._extractors_default

        for extractor in do_extractors:
            try:
                if extractor.does_support_input(request):
                    extractor_result = extractor.do_extract(request)
                    result.append(extractor_result)
            except Exception as e:
                self.throw(e, poll_reference=request.poll_reference)

        return result

    @overrides(AbstractParser)
    def cleanup(self):
        self.log("cleanup")

    @overrides(AbstractParser)
    def static_cleanup(self):
        self.log("static cleanup")
