import logging

from readerwriterlock import rwlock

from dranspose.event import ResultData
from dranspose.protocol import ReducerState


class TestReducer:
    def __init__(self, state: ReducerState | None = None, **kwargs: dict) -> None:
        self.publish: dict[str, dict] = {"results": {}, "parameters": {}}

    def process_result(
        self, result: ResultData, parameters: dict | None = None
    ) -> None:
        logging.info("parameters are %s", parameters)
        self.publish["results"][result.event_number] = result.payload
        self.publish["parameters"][result.event_number] = parameters

    def finish(self, parameters: dict | None = None) -> None:
        print("finished dummy reducer work")


class LockReducer:
    def __init__(self, state: ReducerState | None = None, **kwargs: dict) -> None:
        self.publish: dict[str, dict] = {"results": {}, "parameters": {}}
        self.rw_lock = rwlock.RWLockFair()
        self.publish_rlock = self.rw_lock.gen_rlock()
        self.publish_wlock = self.rw_lock.gen_wlock()

    def process_result(
        self, result: ResultData, parameters: dict | None = None
    ) -> None:
        logging.info("parameters are %s", parameters)
        with self.publish_wlock:
            self.publish["results"][result.event_number] = result.payload
            self.publish["parameters"][result.event_number] = parameters

    def finish(self, parameters: dict | None = None) -> None:
        print("finished dummy reducer work")
