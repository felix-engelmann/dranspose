from dranspose.event import ResultData
from dranspose.parameters import StrParameter, FileParameter


class FluorescenceReducer:
    def __init__(self, parameters=None):
        self.number = 0
        self.publish = {"map": {}}

    @staticmethod
    def describe_parameters():
        params = [
            StrParameter(name="string_parameter"),
            FileParameter(name="other_file_parameter"),
        ]
        return params

    def process_result(self, result: ResultData, parameters=None):
        print(result)
        if result.payload:
            self.publish["map"][result.payload["position"]] = result.payload[
                "concentations"
            ]
