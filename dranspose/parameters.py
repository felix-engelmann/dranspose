from typing import Optional, Literal

from pydantic import BaseModel, TypeAdapter


class Parameter(BaseModel):
    name: str
    description: Optional[str] = None


class StrParameter(Parameter):
    dtype: Literal["str"] = "str"


class FileParameter(Parameter):
    dtype: Literal["file"] = "file"


ParameterList = TypeAdapter(list[StrParameter | FileParameter])
