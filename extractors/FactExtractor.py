from implementation.abstract_extractor import AbstractExtractor
from implementation.model.data_extraction_request import DataExtractionRequest
from connarchitecture.decorators import overrides
from implementation.model.concept import Concept
from implementation.model.ticker import Ticker
import json
import jsonpath_rw_ext


class FactExtractor(AbstractExtractor):
    @overrides(AbstractExtractor)
    def supports_input(self, request: DataExtractionRequest):
        return request.file_extension and request.file_extension.upper() == ".JSON"

    def _find_value_for_concept(self, file: str, fy: int) -> str:
        result = None
        with open(file, 'r') as json_file:
            try:
                json_data = json.load(json_file)
                jsonpath_expression = jsonpath_rw_ext.parse(f'units.USD[?(fy=={fy} & form="10-K")]')
                matches = jsonpath_expression.find(json_data)
                if matches:
                    result = max(matches, key=lambda m: m.value['end']).value['val']
            except:
                print(f'Unable to load json {file}')
        return result

    @overrides(AbstractExtractor)
    def extract(self, request: DataExtractionRequest):
        self.log(f"Proccessing '{request.file}'")
        result = []
        value = self._find_value_for_concept(file=request.file, fy=request.year)
        if value:
            try:
                result.append(
                    Concept(
                        ticker=Ticker(symbol=request.ticker),
                        name=request.concept,
                        year=request.year,
                        value=float(value)
                    )
                )
            except Exception as e:
                self.log_exception(exception=e)

        return result
