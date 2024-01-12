import abc
import logging
from typing import Optional

import redis.asyncio as redis
from pydantic import UUID4, AliasChoices, Field, RedisDsn
from pydantic_core import Url
from pydantic_settings import BaseSettings

from dranspose.helpers.utils import parameters_hash
from dranspose.parameters import Parameter, ParameterType
from dranspose.protocol import (
    RedisKeys,
    ControllerUpdate,
    IngesterState,
    WorkerState,
    ReducerState,
    ParameterName,
    WorkParameter,
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
        self.parameters: dict[ParameterName, WorkParameter] = {}

    async def publish_config(self) -> None:
        self._logger.debug("publish config %s", self.state)
        async with self.redis.pipeline() as pipe:
            if isinstance(self.state, IngesterState):
                category = "ingester"
            elif isinstance(self.state, WorkerState):
                category = "worker"
            elif isinstance(self.state, ReducerState):
                category = "reducer"
            else:
                raise NotImplementedError(
                    "Distributed Service not implemented for your Service"
                )

            await pipe.setex(
                RedisKeys.config(category, self.state.name),
                10,
                self.state.model_dump_json(),
            )
            if hasattr(self, "param_descriptions"):
                for p in self.param_descriptions:
                    self._logger.debug("register parameter %s", p)
                    try:
                        await pipe.setex(
                            RedisKeys.parameter_description(p.name),
                            10,
                            p.model_dump_json(),
                        )
                    except Exception as e:
                        self._logger.error(
                            "failed to register parameter %s", e.__repr__()
                        )

            await pipe.execute()

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
            await self.publish_config()
            try:
                update_msgs = await self.redis.xread(
                    {RedisKeys.updates(): last}, block=6000
                )
                if RedisKeys.updates() in update_msgs:
                    update_msg = update_msgs[RedisKeys.updates()][0][-1]
                    last = update_msg[0]
                    update = ControllerUpdate.model_validate_json(update_msg[1]["data"])
                    self._logger.debug("update type %s", update)
                    newuuid = update.mapping_uuid
                    if newuuid != self.state.mapping_uuid:
                        self._logger.info("resetting config to %s", newuuid)
                        try:
                            await self.restart_work(newuuid)
                        except Exception as e:
                            self._logger.error("restart_work failed %s", e.__repr__())
                    paramuuids = update.parameters_version
                    for name in paramuuids:
                        if (
                            name not in self.parameters
                            or self.parameters[name] != paramuuids[name]
                        ):
                            try:
                                params = await self.raw_redis.get(
                                    RedisKeys.parameters(name, paramuuids[name])
                                )
                                self._logger.debug(
                                    "received binary parameters for %s", name
                                )
                                if params:
                                    self._logger.info(
                                        "set parameter %s of length %s",
                                        name,
                                        len(params),
                                    )
                                    self.parameters[name] = WorkParameter(
                                        name=name, uuid=paramuuids[name], data=params
                                    )
                                    # check if this parameter has a description and type
                                    desc = await self.redis.get(
                                        RedisKeys.parameter_description(name),
                                    )
                                    self._logger.debug("description is %s", desc)
                                    if desc:
                                        param_desc: ParameterType = Parameter.validate_json(desc)  # type: ignore
                                        self._logger.info(
                                            "set paremter has a description %s",
                                            param_desc,
                                        )
                                        self.parameters[
                                            name
                                        ].value = param_desc.from_bytes(params)
                                        self._logger.debug(
                                            "parsed parameter value %s",
                                            self.parameters[name].value,
                                        )
                            except Exception as e:
                                self._logger.error(
                                    "failed to get parameters %s", e.__repr__()
                                )
                    self.state.parameters_hash = parameters_hash(self.parameters)
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
