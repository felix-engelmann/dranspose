import time
from typing import NewType, Any, Literal

from pydantic import BaseModel, Field, field_validator

H5UUID = NewType("H5UUID", str)


class H5Times(BaseModel):
    created: float = Field(default_factory=time.time)
    lastModified: float = Field(default_factory=time.time)


class H5Response(H5Times):
    root: H5UUID


class H5Root(H5Response):
    owner: str = "admin"
    class_: str = Field("domain", alias="class")


class H5Group(H5Response):
    id: H5UUID
    linkCount: int = 0
    attributeCount: int = 0


class H5StrType(BaseModel):
    class_: Literal["H5T_STRING"] = Field("H5T_STRING", alias="class")
    length: Literal["H5T_VARIABLE"] = "H5T_VARIABLE"
    charSet: Literal["H5T_CSET_UTF8"] = "H5T_CSET_UTF8"
    strPad: Literal["H5T_STR_NULLTERM"] = "H5T_STR_NULLTERM"


class H5NumBase(BaseModel):
    base: str

    @field_validator("base")
    @classmethod
    def base_must_start(cls, v: str) -> str:
        if not v.startswith("H5T_"):
            raise ValueError("type base must start with H5T_")
        return v


class H5IntType(H5NumBase):
    class_: Literal["H5T_INTEGER"] = Field("H5T_INTEGER", alias="class")


class H5FloatType(H5NumBase):
    class_: Literal["H5T_FLOAT"] = Field("H5T_FLOAT", alias="class")


class H5CompType(BaseModel):
    class_: Literal["H5T_COMPOUND"] = Field("H5T_COMPOUND", alias="class")
    fields: "list[H5NamedType]"


H5Type = H5StrType | H5FloatType | H5IntType | H5CompType


class H5NamedType(BaseModel):
    name: str
    type: H5Type


class H5ScalarShape(BaseModel):
    class_: Literal["H5S_SCALAR"] = Field("H5S_SCALAR", alias="class")


class H5SimpleShape(BaseModel):
    class_: Literal["H5S_SIMPLE"] = Field("H5S_SIMPLE", alias="class")
    dims: list[int]


H5Shape = H5SimpleShape | H5ScalarShape


class H5Attribute(H5Times):
    name: str
    type: H5Type
    shape: H5Shape


class H5ValuedAttribute(H5Attribute):
    value: Any


class H5Attributes(BaseModel):
    attributes: list[H5Attribute]


class H5Link(H5Times):
    class_: Literal["H5L_TYPE_HARD"] = Field("H5L_TYPE_HARD", alias="class")
    collection: Literal["datasets", "groups"]
    id: H5UUID
    title: str


class H5Dataset(H5Times):
    id: H5UUID
    attributeCount: int = 0
    shape: H5Shape
    type: H5Type
    creationProperties: dict[str, str | dict[str, str]] = {
        "allocTime": "H5D_ALLOC_TIME_LATE",
        "fillTime": "H5D_FILL_TIME_IFSET",
        "layout": {"class": "H5D_CONTIGUOUS"},
    }
