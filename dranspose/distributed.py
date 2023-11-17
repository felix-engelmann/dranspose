import abc
import logging
from typing import Literal

import redis.asyncio as redis
from pydantic import UUID4, AliasChoices, Field, RedisDsn
from pydantic_core import Url
from pydantic_settings import BaseSettings

from dranspose.protocol import (
    RedisKeys,
    ControllerUpdate,
    IngesterState,
    WorkerState, WorkerName, IngesterName,
)
import redis.exceptions as rexceptions
import asyncio


class DistributedSettings(BaseSettings):
    redis_dsn: RedisDsn = Field(
        Url('redis://localhost:6379/0'),
        validation_alias=AliasChoices('service_redis_dsn', 'redis_url')
    )


class DistributedService(abc.ABC):
    def __init__(self, state: WorkerState|IngesterState, settings: DistributedSettings | None = None):

        self._distributed_settings = settings
        if self._distributed_settings is None:
            self._distributed_settings = DistributedSettings()

        self.state: WorkerState | IngesterState = state
        if ":" in state.name:
            raise Exception("Worker name must not contain a :")
        # TODO: check for already existing query string
        self.redis = redis.from_url(f"{self._distributed_settings.redis_dsn}?decode_responses=True&protocol=3")
        self._logger = logging.getLogger(f"{__name__}+{self.state.name}")

    async def register(self) -> None:
        latest = await self.redis.xrevrange(RedisKeys.updates(), count=1)
        last = 0
        if len(latest) > 0:
            last = latest[0][0]
        while True:
            if isinstance(self.state, IngesterState):
                category = "ingester"
            elif isinstance(self.state, WorkerState):
                category = "worker"
            else:
                raise NotImplemented(
                    "Distributed Service not implemented for your Service"
                )
            await self.redis.setex(
                RedisKeys.config(category, self.state.name),
                10,
                self.state.model_dump_json(),
            )
            try:
                update = await self.redis.xread({RedisKeys.updates(): last}, block=6000)
                if RedisKeys.updates() in update:
                    update = update[RedisKeys.updates()][0][-1]
                    last = update[0]
                    update = ControllerUpdate.model_validate(update[1])
                    newuuid = update.mapping_uuid
                    if newuuid != self.state.mapping_uuid:
                        self._logger.info("resetting config to %s", newuuid)
                        await self.restart_work(newuuid)
            except rexceptions.ConnectionError:
                break
            except asyncio.exceptions.CancelledError:
                break
