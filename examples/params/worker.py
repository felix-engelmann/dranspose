from typing import Any

from dranspose.parameters import (
    ParameterBase,
    StrParameter,
    BinaryParameter,
    IntParameter,
    FloatParameter,
    BoolParameter,
)
from dranspose.protocol import ParameterName


class ParamWorker:
    def __init__(self, **kwargs: Any) -> None:
        self.number = 0

    @staticmethod
    def describe_parameters() -> list[ParameterBase]:
        params = [
            StrParameter(name=ParameterName("roi1"), default="bla"),
            StrParameter(name=ParameterName("str_param")),
            BinaryParameter(name=ParameterName("bytes_param")),
            IntParameter(name=ParameterName("int_param")),
            FloatParameter(name=ParameterName("float_param")),
            BoolParameter(name=ParameterName("bool_param")),
        ]
        return params

    def process_event(self, event, parameters, *args, **kwargs):
        return parameters
