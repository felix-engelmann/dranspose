import asyncio
import json
import logging
import pickle
import traceback
from contextlib import asynccontextmanager, nullcontext
from typing import ContextManager, Optional, AsyncGenerator, Any, Tuple, Annotated

import zmq.asyncio
from fastapi import FastAPI, Body
from pydantic import UUID4, BaseModel
from starlette.requests import Request

from dranspose.helpers import utils
from dranspose.distributed import DistributedService, DistributedSettings
from dranspose.event import ResultData
from dranspose.helpers.h5dict import router
from dranspose.helpers.utils import done_callback, cancel_and_wait
from dranspose.protocol import (
    ReducerState,
    ZmqUrl,
    RedisKeys,
    ReducerUpdate,
    DistributedStateEnum,
    StreamName,
)

logger = logging.getLogger(__name__)


class ReducerSettings(DistributedSettings):
    reducer_url: ZmqUrl = ZmqUrl("tcp://localhost:10200")
    reducer_class: Optional[str] = None


class Reducer(DistributedService):
    def __init__(self, settings: Optional[ReducerSettings] = None):
        self._reducer_settings = settings
        if self._reducer_settings is None:
            self._reducer_settings = ReducerSettings()

        state = ReducerState(url=self._reducer_settings.reducer_url)
        super().__init__(state, self._reducer_settings)
        self._logger.info("created reducer with state %s", state)
        self.state: ReducerState
        self.ctx = zmq.asyncio.Context()
        self.in_socket = self.ctx.socket(zmq.PULL)
        self.in_socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.in_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
        self.in_socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 300)
        self.in_socket.bind(f"tcp://*:{self._reducer_settings.reducer_url.port}")

        self.custom = None
        self.custom_context: dict[Any, Any] = {}
        self.parameter_class: type | None = None
        self.parameter_object = None
        if self._reducer_settings.reducer_class:
            try:
                self.custom = utils.import_class(self._reducer_settings.reducer_class)
                self._logger.info("custom reducer class %s", self.custom)
                try:
                    pcl = self.custom.parameter_class
                    if issubclass(pcl, BaseModel):
                        self.parameter_class = pcl
                        self.parameter_object = pcl()
                    else:
                        self._logger.error("parameter_class is not a BaseModel")
                    self._logger.info("parameter class is %s", pcl)
                except Exception as e:
                    self._logger.warning(
                        "could not create parameter class %s", e.__repr__()
                    )
                try:
                    self.param_descriptions = self.custom.describe_parameters()  # type: ignore[attr-defined]
                except AttributeError:
                    self._logger.info(
                        "custom worker class has no describe_parameters staticmethod"
                    )
                except Exception as e:
                    self._logger.error(
                        "custom worker parameter descripition is broken: %s",
                        e.__repr__(),
                    )
            except Exception as e:
                self._logger.warning(
                    "failed to load custom reducer class, discarding results %s trace: %s",
                    e.__repr__(),
                    traceback.format_exc(),
                )

    async def run(self) -> None:
        self.work_task = asyncio.create_task(self.work())
        self.work_task.add_done_callback(done_callback)
        self.timer_task = asyncio.create_task(self.timer())
        self.timer_task.add_done_callback(done_callback)
        self.metrics_task = asyncio.create_task(self.update_metrics())
        self.metrics_task.add_done_callback(done_callback)
        await self.register()

    async def work(self) -> None:
        self._logger.info("started work task")
        self.reducer = None
        if self.custom:
            self.reducer = self.custom(
                parameters=self.parameters,
                context=self.custom_context,
                state=self.state,
            )
        while True:
            parts = await self.in_socket.recv_multipart()
            prelim = json.loads(parts[0])
            prelim["payload"] = pickle.loads(parts[1])
            result = ResultData.model_validate(prelim)
            if self.reducer:
                try:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None, self.reducer.process_result, result, self.parameters
                    )
                except Exception as e:
                    self._logger.error(
                        "custom reducer failed: %s\n%s",
                        e.__repr__(),
                        traceback.format_exc(),
                    )
            ru = ReducerUpdate(
                state=DistributedStateEnum.IDLE,
                completed=result.event_number,
                worker=result.worker,
            )
            self._logger.debug("result processed, notify controller with %s", ru)
            await self.redis.xadd(
                RedisKeys.ready(self.state.mapping_uuid),
                {"data": ru.model_dump_json()},
            )
            self._logger.debug("processed result %s", result)
            self.state.processed_events += 1

    async def timer(self) -> None:
        self._logger.info("started timer task")
        while True:
            delay = 1
            if self.reducer:
                if hasattr(self.reducer, "timer"):
                    try:
                        loop = asyncio.get_event_loop()
                        delay = await loop.run_in_executor(None, self.reducer.timer)
                    except Exception as e:
                        self._logger.error(
                            "custom reducer timer failed: %s\n%s",
                            e.__repr__(),
                            traceback.format_exc(),
                        )

            if not isinstance(delay, (int, float)):
                self._logger.error(
                    "custom reducer time is not a number: %s",
                    delay,
                )
                delay = 1
            await asyncio.sleep(delay)

    async def restart_work(
        self, new_uuid: UUID4, active_streams: list[StreamName]
    ) -> None:
        self._logger.info("resetting config %s", new_uuid)
        await cancel_and_wait(self.timer_task)
        await cancel_and_wait(self.work_task)
        self.state.mapping_uuid = new_uuid
        self.work_task = asyncio.create_task(self.work())
        self.work_task.add_done_callback(done_callback)
        self.timer_task = asyncio.create_task(self.timer())
        self.timer_task.add_done_callback(done_callback)

    async def finish_work(self) -> None:
        self._logger.info("finishing reducer work")
        if self.reducer:
            if hasattr(self.reducer, "finish"):
                try:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None, self.reducer.finish, self.parameters
                    )
                except Exception as e:
                    self._logger.error(
                        "custom reducer finish failed: %s\n%s",
                        e.__repr__(),
                        traceback.format_exc(),
                    )
        await self.redis.xadd(
            RedisKeys.ready(self.state.mapping_uuid),
            {
                "data": ReducerUpdate(
                    state=DistributedStateEnum.FINISHED,
                ).model_dump_json()
            },
        )

    async def close(self) -> None:
        if self.reducer:
            if hasattr(self.reducer, "close"):
                try:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None, self.reducer.close, self.custom_context
                    )
                except Exception as e:
                    self._logger.error(
                        "custom reducer failed to close: %s\n%s",
                        e.__repr__(),
                        traceback.format_exc(),
                    )
        await cancel_and_wait(self.timer_task)
        await cancel_and_wait(self.work_task)
        await cancel_and_wait(self.metrics_task)
        await self.redis.delete(RedisKeys.config("reducer", self.state.name))
        await super().close()
        self.ctx.destroy(linger=0)
        self._logger.info("closed reducer")

    async def get_params(self) -> Any:
        return self.parameter_object


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Load the ML model
    app.state.reducer = Reducer()
    run_task = asyncio.create_task(app.state.reducer.run())
    run_task.add_done_callback(done_callback)

    def get_data() -> Tuple[dict[str, Any], ContextManager[None]]:
        data = {}
        lock = nullcontext()
        if app.state.reducer.reducer is not None:
            if hasattr(app.state.reducer.reducer, "publish"):
                data = app.state.reducer.reducer.publish
            if hasattr(app.state.reducer.reducer, "publish_rlock"):
                lock = app.state.reducer.reducer.publish_rlock
        return data, lock

    if app.state.reducer.parameter_class is not None:

        async def set_params(
            request: Request, data: Annotated[app.state.reducer.parameter_class, Body()]
        ) -> dict[str, list[str]]:
            update_data = data.model_dump(exclude_unset=True)
            logger.debug("update data %s", update_data)
            request.app.state.reducer.parameter_object = (
                request.app.state.reducer.parameter_object.model_copy(
                    update=update_data
                )
            )
            return {"updated_keys": list(update_data.keys())}

        app.add_api_route(
            "/api/v1/parameters",
            app.state.reducer.get_params,
            methods=["GET"],
            response_model=app.state.reducer.parameter_class,
        )
        app.add_api_route("/api/v1/parameters", set_params, methods=["POST"])

    app.state.get_data = get_data
    yield
    await cancel_and_wait(run_task)
    await app.state.reducer.close()
    # Clean up the ML models and release the resources


app = FastAPI(lifespan=lifespan)

app.include_router(router)


@app.get("/api/v1/status")
async def get_status() -> bool:
    return True
