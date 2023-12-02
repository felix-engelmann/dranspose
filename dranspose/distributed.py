import abc
import logging
import pickle
from typing import Literal, Optional

import redis.asyncio as redis
from pydantic import UUID4, AliasChoices, Field, RedisDsn
from pydantic_core import Url
from pydantic_settings import BaseSettings

from dranspose.protocol import (
    RedisKeys,
    ControllerUpdate,
    IngesterState,
    WorkerState,
    WorkerName,
    IngesterName,
    ReducerState,
)
import redis.exceptions as rexceptions
import asyncio


class DistributedSettings(BaseSettings):
    """
    Basic settings for any distributed service

    Attributes:
        redis_dsn: URL of the common redis instance
    """

    redis_dsn: RedisDsn = Field(
        Url("redis://localhost:6379/0"),
        validation_alias=AliasChoices("service_redis_dsn", "redis_url"),
    )


class DistributedService(abc.ABC):
    """
    Abstract class defining common functionality for all services.
    """

    def __init__(
        self,
        state: WorkerState | IngesterState | ReducerState,
        settings: Optional[DistributedSettings] = None,
    ):
        self._distributed_settings = settings
        if self._distributed_settings is None:
            self._distributed_settings = DistributedSettings()

        self.state: WorkerState | IngesterState | ReducerState = state

        if ":" in state.name:
            raise Exception("Worker name must not contain a :")
        # TODO: check for already existing query string
        self.redis = redis.from_url(
            f"{self._distributed_settings.redis_dsn}?decode_responses=True&protocol=3"
        )
        self.raw_redis = redis.from_url(
            f"{self._distributed_settings.redis_dsn}?protocol=3"
        )
        self._logger = logging.getLogger(f"{__name__}+{self.state.name}")
        self.parameters = None

    async def register(self) -> None:
        """
        Background job in every distributed service to publish the service's configuration.
        It publishes the `state` every 6 seconds or faster if there are updates from the controller with a new trigger map or parameters.
        """
        latest = await self.redis.xrevrange(RedisKeys.updates(), count=1)
        last = 0
        if len(latest) > 0:
            last = latest[0][0]
        while True:
            if isinstance(self.state, IngesterState):
                category = "ingester"
            elif isinstance(self.state, WorkerState):
                category = "worker"
            elif isinstance(self.state, ReducerState):
                category = "reducer"
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
                    update = ControllerUpdate.model_validate_json(update[1]["data"])
                    self._logger.debug("update type %s", update)
                    newuuid = update.mapping_uuid
                    if newuuid != self.state.mapping_uuid:
                        self._logger.info("resetting config to %s", newuuid)
                        await self.restart_work(newuuid)
                    newuuid = update.parameters_uuid
                    if newuuid != self.state.parameters_uuid:
                        self._logger.info("setting parameters to %s", newuuid)
                        try:
                            params = await self.raw_redis.get(
                                RedisKeys.parameters(newuuid)
                            )
                        except Exception as e:
                            self._logger.error(
                                "failed to get parameters %s", e.__repr__()
                            )
                        self._logger.debug("received binary parameters")
                        if params:
                            self._logger.error("set parameters %s", len(params))
                            self.parameters = pickle.loads(params)
                            self.state.parameters_uuid = newuuid
                    if update.finished:
                        self._logger.info("finished messages")
                        await self.finish_work()

            except rexceptions.ConnectionError:
                break
            except asyncio.exceptions.CancelledError:
                break

    async def restart_work(self, uuid: UUID4) -> None:
        pass

    async def finish_work(self) -> None:
        pass

    async def close(self) -> None:
        await self.redis.aclose()
        await self.raw_redis.aclose()
