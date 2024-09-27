from typing import Optional, Literal, Any

from pydantic import BaseModel, TypeAdapter

from dranspose.protocol import ParameterName


class ParameterBase(BaseModel):
    name: ParameterName
    description: Optional[str] = None

    @staticmethod
    def to_bytes(val: Any) -> bytes:
        return str(val).encode("utf8")


class StrParameter(ParameterBase):
    dtype: Literal["str"] = "str"
    default: str = ""

    @staticmethod
    def from_bytes(by: bytes) -> str:
        try:
            return by.decode("utf8")
        except ValueError:
            return ""


class BinaryParameter(ParameterBase):
    dtype: Literal["binary"] = "binary"
    default: bytes = b""

    @staticmethod
    def from_bytes(by: bytes) -> bytes:
        return by

    @staticmethod
    def to_bytes(val: Any) -> bytes:
        return val


class IntParameter(ParameterBase):
    dtype: Literal["int"] = "int"
    default: int = 0

    @staticmethod
    def from_bytes(by: bytes) -> int:
        try:
            return int(by)
        except ValueError:
            return 0


class FloatParameter(ParameterBase):
    dtype: Literal["float"] = "float"
    default: float = 0.0

    @staticmethod
    def from_bytes(by: bytes) -> float:
        try:
            return float(by)
        except ValueError:
            return 0.0


class BoolParameter(ParameterBase):
    dtype: Literal["bool"] = "bool"
    default: bool = False

    @staticmethod
    def from_bytes(by: bytes) -> bool:
        try:
            return by == b"True"
        except ValueError:
            return False


ParameterType = (
    StrParameter | BinaryParameter | IntParameter | FloatParameter | BoolParameter
)

Parameter = TypeAdapter(ParameterType)

ParameterList = TypeAdapter(list[ParameterType])
