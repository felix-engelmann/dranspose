from typing import Optional, Literal

from pydantic import BaseModel, TypeAdapter


class ParameterBase(BaseModel):
    name: str
    description: Optional[str] = None


class StrParameter(ParameterBase):
    dtype: Literal["str"] = "str"


class FileParameter(ParameterBase):
    dtype: Literal["file"] = "file"


class IntParameter(ParameterBase):
    dtype: Literal["int"] = "int"


class FloatParameter(ParameterBase):
    dtype: Literal["float"] = "float"


ParameterType = StrParameter | FileParameter | IntParameter | FloatParameter

Parameter = TypeAdapter(ParameterType)

ParameterList = TypeAdapter(list[ParameterType])
