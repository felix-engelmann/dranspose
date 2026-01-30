import abc
import logging
import os
import time
from typing import Optional, Any
from importlib.metadata import version

import redis.asyncio as redis
from pydantic import UUID4, AliasChoices, Field, RedisDsn
from pydantic_core import Url
from pydantic_settings import BaseSettings

from dranspose.helpers.utils import parameters_hash, cancel_and_wait
from dranspose.parameters import Parameter, ParameterType
from dranspose.protocol import (
    RedisKeys,
    ControllerUpdate,
    IngesterState,
    WorkerState,
    ReducerState,
    ParameterName,
    WorkParameter,
    BuildGitMeta,
    StreamName,
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

    build_meta_file: Optional[os.PathLike[Any] | str] = "/etc/build_git_meta.json"


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
        self._logger.info("log handlers are %s", self._logger.handlers)
        try:
            if self._distributed_settings.build_meta_file is not None:
                with open(self._distributed_settings.build_meta_file) as fd:
                    build_data = BuildGitMeta.model_validate_json(fd.read())
                    self._logger.info("build meta is %s", build_data)
                    self.state.mapreduce_version = build_data
        except Exception as e:
            logging.info("cannot load build meta information: %s", e.__repr__())
        self.state.dranspose_version = version("dranspose")
        self._logger.info("running version %s", self.state.dranspose_version)
        self.parameters: dict[ParameterName, WorkParameter] = {}

    def get_category(self) -> str:
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
        return category

    async def publish_config(self) -> None:
        self._logger.debug("publish config %s", self.state)
        async with self.redis.pipeline() as pipe:
            await pipe.setex(
                RedisKeys.config(self.get_category(), self.state.name),
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
        It publishes the `state` every 1.3 seconds or faster if there are updates from the controller with a new trigger map or parameters.
        """
        latest = await self.redis.xrevrange(RedisKeys.updates(), count=1)
        last = 0
        if len(latest) > 0:
            last = latest[0][0]
        while True:
            await self.publish_config()
            try:
                update_msgs = await self.redis.xread(
                    {RedisKeys.updates(): last}, block=1300
                )
                if RedisKeys.updates() in update_msgs:
                    update_msg = update_msgs[RedisKeys.updates()][0][-1]
                    last = update_msg[0]
                    update = ControllerUpdate.model_validate_json(update_msg[1]["data"])
                    self._logger.debug("update type %s", update)
                    newuuid = update.mapping_uuid
                    if newuuid != self.state.mapping_uuid:
                        self._logger.info(
                            "resetting config to %s with streams %s",
                            newuuid,
                            update.active_streams,
                        )
                        try:
                            await self.restart_work(newuuid, update.active_streams)
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
                                if params is not None:
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
                                        param_desc: ParameterType = (
                                            Parameter.validate_json(desc)
                                        )
                                        self._logger.info(
                                            "set parameter has a description %s",
                                            param_desc,
                                        )
                                        self.parameters[
                                            name
                                        ].value = param_desc.from_bytes(params)
                                        self._logger.debug(
                                            "parsed parameter value %s",
                                            self.parameters[name].value,
                                        )
                            except ConnectionError as e:
                                self._logger.warning(
                                    "cannot connect to fetch parameters %s",
                                    e.__repr__(),
                                )
                            except Exception as e:
                                self._logger.error(
                                    "failed to get parameters %s", e.__repr__()
                                )
                    self.state.parameters_hash = parameters_hash(self.parameters)
                    self._logger.debug(
                        "set local parameters to %s with hash %s",
                        {n: p.uuid for n, p in self.parameters.items()},
                        self.state.parameters_hash,
                    )
                    if update.finished:
                        self._logger.info("finished messages")
                        await self.finish_work()

            except rexceptions.ConnectionError:
                break
            except asyncio.exceptions.CancelledError:
                break

    async def multiprocess_run(self, queue: Any) -> None:
        task = asyncio.create_task(self.run())
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, queue.get)
        await self.close()
        await cancel_and_wait(task)

    def sync_run(self, queue: Any) -> None:
        asyncio.run(self.multiprocess_run(queue))

    async def update_metrics(self) -> None:
        while True:
            start = time.time()
            old = self.state.processed_events
            await asyncio.sleep(1.0)
            end = time.time()
            rate = (self.state.processed_events - old) / (end - start)
            self.state.event_rate = rate
            if (self.state.processed_events - old) > 0:
                self._logger.info("receiving %lf frames per second", rate)

    async def run(self) -> None:
        pass

    async def restart_work(self, uuid: UUID4, active_streams: list[StreamName]) -> None:
        pass

    async def finish_work(self) -> None:
        pass

    async def close(self) -> None:
        await self.redis.delete(RedisKeys.config(self.get_category(), self.state.name))
        self._logger.info(
            "deleted redis key %s",
            RedisKeys.config(self.get_category(), self.state.name),
        )
        await self.redis.aclose()
        await self.raw_redis.aclose()
