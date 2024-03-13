from typing import Literal

from pydantic import BaseModel, TypeAdapter, ConfigDict


class EigerLegacy(BaseModel):
    pass


class EigerLegacyHeader(EigerLegacy):
    """
    Example:
        ``` py
        EigerLegacyHeader(
            htype='header',
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["dheader-1.0"]


class EigerLegacyImage(EigerLegacy):
    """
    Example:
        ``` py
        EigerLegacyImage(
            htype='dimage-1.0',
            frame=0
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["dimage-1.0"]
    frame: int


class EigerLegacyEnd(EigerLegacy):
    """
    Example:
        ``` py
        EigerLegacyEnd(
            htype='dseries_end-1.0'
        )
        ```
    """

    model_config = ConfigDict(extra="allow")

    htype: Literal["dseries_end-1.0"]


EigerLegacyPacket = TypeAdapter(EigerLegacyHeader | EigerLegacyImage | EigerLegacyEnd)
"""
A union type for Eiger Legacy packets
"""
