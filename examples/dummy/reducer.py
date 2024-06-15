from dranspose.event import ResultData
from dranspose.parameters import StrParameter, FileParameter


class FluorescenceReducer:
    def __init__(self, state=None, **kwargs):
        self.number = 0
        self.publish = {"map": {}}
        if state is not None and state.mapreduce_version is not None:
            self.publish["version"] = state.mapreduce_version.model_dump(mode="json")

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
