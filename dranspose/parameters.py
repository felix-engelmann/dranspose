import asyncio
import hashlib
import logging
from typing import Any

from pydantic import BaseModel
import redis.exceptions as rexceptions


from dranspose.protocol import HashDigest, RedisKeys, ParameterState, ParameterUpdate


class DistributedModel:
    def __init__(self, model, redis, channel):
        self.model: BaseModel = model
        self.channel = channel
        self.data = model()
        self.state = ParameterState(parameter_signature=str(self.model.__signature__))
        self._update_hash()
        self.redis = redis

    def _update_hash(self):
        res = self.data.model_dump_json()
        m = hashlib.sha256()
        m.update(res.encode())
        self.state.parameter_hash = HashDigest(m.hexdigest())

    async def patch(self, update: dict[str, Any]):
        partial = self.model.model_validate(update)
        update_validated = partial.model_dump(exclude_unset=True)
        self.data = self.data.model_copy(update=update_validated)

        self._update_hash()
        update_json = partial.model_dump_json(exclude_unset=True)
        update_msg = ParameterUpdate(
            update=update_json, parameter_hash_target=self.state.parameter_hash
        )
        await self.redis.xadd(
            RedisKeys.parameter_updates(self.channel),
            {"data": update_msg.model_dump_json()},
        )

    async def aclose(self):
        await self.redis.delete(RedisKeys.parameter_updates(self.channel))

    async def refresh(self):
        update_msg = ParameterUpdate(
            update=self.data.model_dump_json(),
            parameter_hash_target=self.state.parameter_hash,
        )
        await self.redis.xadd(
            RedisKeys.parameter_updates(self.channel),
            {"data": update_msg.model_dump_json()},
        )

    def is_consistent(self, states: list[ParameterState]):
        for state in states:
            if state != self.state:
                return False
        return True

    async def subscribe(self, callback=None):
        latest_parameter = await self.redis.xrevrange(
            RedisKeys.parameter_updates(self.channel), count=1
        )
        last_parameter = 0
        if len(latest_parameter) > 0:
            last_parameter = latest_parameter[0][0]

        while True:
            try:
                update_msgs = await self.redis.xread(
                    {RedisKeys.parameter_updates(self.channel): last_parameter},
                    block=1300,
                )
                if RedisKeys.parameter_updates(self.channel) in update_msgs:
                    for update_msg in update_msgs[
                        RedisKeys.parameter_updates(self.channel)
                    ][0]:
                        last_parameter = update_msg[0]
                        update = ParameterUpdate.model_validate_json(
                            update_msg[1]["data"]
                        )
                        logging.info("received parameter update: %s", update)
                        created = self.model.model_validate_json(update.update)
                        new_things = created.model_dump(exclude_unset=True)
                        logging.info("created update %s", new_things)
                        self.data = self.data.model_copy(update=new_things)
                    self._update_hash()
                    if callback:
                        await callback()
            except rexceptions.ConnectionError:
                break
            except asyncio.exceptions.CancelledError:
                break
