from typing import Literal

from pydantic import UUID4, AliasChoices, Field, RedisDsn
from pydantic_settings import BaseSettings

from dranspose.protocol import (
    RedisKeys,
    ControllerUpdate,
    IngesterState,
    WorkerState,
)
import redis.exceptions as rexceptions
import asyncio


class Settings(BaseSettings):
    redis_dsn: RedisDsn = Field(
        'redis://localhost:6379/0',
        validation_alias=AliasChoices('service_redis_dsn', 'redis_url')
    )


class DistributedService:
    def __init__(self):
        self.settings = Settings()
        print("self.settings", self.settings)
        self.redis = None
        self.state = None
        self._logger = None

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

    async def restart_work(self, new_uuid: UUID4):
        raise NotImplemented("restart work needs to be implemented")