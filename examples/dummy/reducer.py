from dranspose.event import ResultData
from dranspose.parameters import StrParameter, FileParameter


class FluorescenceReducer:
    def __init__(self, **kwargs):
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

    def finish(self, parameters=None):
        print("finished dummy reducer work")
