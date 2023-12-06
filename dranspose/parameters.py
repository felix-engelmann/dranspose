from typing import Optional, Literal

from pydantic import BaseModel, TypeAdapter


class ParameterBase(BaseModel):
    name: str
    description: Optional[str] = None


class StrParameter(ParameterBase):
    dtype: Literal["str"] = "str"


class FileParameter(ParameterBase):
    dtype: Literal["file"] = "file"


Parameter = TypeAdapter(StrParameter | FileParameter)

ParameterList = TypeAdapter(list[StrParameter | FileParameter])
