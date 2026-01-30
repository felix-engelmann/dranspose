from typing import Any
from dranspose.event import ResultData
from dranspose.parameters import ParameterType, StrParameter, BinaryParameter
from dranspose.protocol import ParameterName, ReducerState


class FluorescenceReducer:
    def __init__(
        self, state: ReducerState | None = None, **kwargs: dict[str, Any]
    ) -> None:
        self.number = 0
        self.publish: dict[str, dict[str, Any]] = {
            "map": {"x": [], "y": [], "data": {}}
        }
        if state is not None and state.mapreduce_version is not None:
            self.publish["version"] = state.mapreduce_version.model_dump(mode="json")

    @staticmethod
    def describe_parameters() -> list[ParameterType]:
        params = [
            StrParameter(name=ParameterName("string_parameter")),
            BinaryParameter(name=ParameterName("other_file_parameter")),
        ]
        return params

    def process_result(
        self, result: ResultData, parameters: dict | None = None
    ) -> None:
        print(result)
        if result.payload:
            self.publish["map"]["x"].append(result.payload["position"][0])
            self.publish["map"]["y"].append(result.payload["position"][1])
            for key, val in result.payload["concentations"].items():
                if key not in self.publish["map"]["data"]:
                    self.publish["map"]["data"][key] = []
                self.publish["map"]["data"][key].append(val)

    def finish(self, parameters: dict | None = None) -> None:
        print("finished dummy reducer work")
