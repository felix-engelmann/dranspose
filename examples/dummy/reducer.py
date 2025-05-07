from dranspose.event import ResultData
from dranspose.parameters import StrParameter, BinaryParameter


class FluorescenceReducer:
    def __init__(self, state=None, **kwargs):
        self.number = 0
        self.publish = {"map": {"x": [], "y": [], "data": {}}}
        if state is not None and state.mapreduce_version is not None:
            self.publish["version"] = state.mapreduce_version.model_dump(mode="json")

    @staticmethod
    def describe_parameters():
        params = [
            StrParameter(name="string_parameter"),
            BinaryParameter(name="other_file_parameter"),
        ]
        return params

    def process_result(self, result: ResultData, parameters=None):
        print(result)
        if result.payload:
            self.publish["map"]["x"].append(result.payload["position"][0])
            self.publish["map"]["y"].append(result.payload["position"][1])
            for key, val in result.payload["concentations"].items():
                if key not in self.publish["map"]["data"]:
                    self.publish["map"]["data"][key] = []
                self.publish["map"]["data"][key].append(val)

    def finish(self, parameters=None):
        print("finished dummy reducer work")
