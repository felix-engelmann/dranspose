import json
import uuid
from collections import defaultdict
from typing import List, Dict, Union, Literal, Optional

from pydantic import validate_call

from dranspose.protocol import (
    WorkAssignment,
    StreamName,
    VirtualWorker,
    WorkerName,
    EventNumber,
)


class NotYetAssigned(Exception):
    pass


class Mapping:
    @validate_call
    def __init__(
        self, m: Dict[StreamName, List[Optional[List[VirtualWorker] | Literal["all"]]]]
    ) -> None:
        if len(set(map(len, m.values()))) > 1:
            raise Exception("length not equal: ", list(map(len, m.values())))

        self.mapping = m
        self.uuid = uuid.uuid4()
        self.assignments: dict[VirtualWorker, WorkerName] = {}
        self.all_assignments: dict[
            tuple[EventNumber, StreamName], list[WorkerName]
        ] = defaultdict(list)
        self.complete_events = 0

    def len(self) -> int:
        if len(self.mapping) > 0:
            return len(list(self.mapping.values())[0])
        else:
            return 0

    def assign_next(
        self, worker: WorkerName, all_workers: list[WorkerName]
    ) -> VirtualWorker | tuple[EventNumber, StreamName] | None:
        for evnint in range(self.complete_events, self.len()):
            evn = EventNumber(evnint)
            for stream, v in self.mapping.items():
                assign = v[evn]
                if assign is not None:
                    if assign == "all":
                        if worker in (
                            set(all_workers) - set(self.all_assignments[(evn, stream)])
                        ):
                            # assign worker to the "all" of the current stream as it is not yet in it
                            self.all_assignments[(evn, stream)].append(worker)
                            self.update_filled(all_workers)
                            return evn, stream
                        else:
                            continue  # maybe the worker is needed in the next stream
                    elif isinstance(assign, list):  # not all
                        for w in assign:
                            if w not in self.assignments:
                                self.assignments[w] = worker
                                self.update_filled(all_workers)
                                return w
        return None

    def min_workers(self) -> int:
        minimum = 0
        for evn, i in enumerate(zip(*self.mapping.values())):
            workers = set()
            for val in i:
                if val is not None:
                    workers.update(val)
            minimum = max(minimum, len(workers))
        return minimum

    def get_event_workers(self, no: EventNumber) -> WorkAssignment:
        ret = {}
        if no > self.complete_events - 1:
            raise NotYetAssigned()
        for s, v in self.mapping.items():
            assign = v[no]
            if assign is None:
                continue
            elif assign == "all":
                ret[s] = self.all_assignments[(no, s)]
            elif isinstance(assign, list):
                ret[s] = [self.assignments[x] for x in assign]
        return WorkAssignment(event_number=no, assignments=ret)

    def update_filled(self, all_workers: list[WorkerName]) -> None:
        for evnint in range(self.complete_events, self.len()):
            evn = EventNumber(evnint)
            for stream, v in self.mapping.items():
                assign = v[evn]
                if assign is None:
                    continue
                elif assign == "all":
                    if set(self.all_assignments[(evn, stream)]) != set(all_workers):
                        return
                else:
                    for w in assign:
                        if w not in self.assignments:
                            return
            self.complete_events = max(0, evn + 1)

    def print(self) -> None:
        print(" " * 5, end="")
        for name in self.mapping:
            print(name.rjust(20), end="")
        print("")
        i: tuple[List[VirtualWorker] | None]
        for evnint in range(self.len()):
            evn = EventNumber(evnint)
            print(str(evn + 1).rjust(5), end="")
            for stream in self.mapping:
                val = self.mapping[stream][evn]
                if val is None:
                    print("None".rjust(20), end="")
                    continue
                elif val == "all":
                    print(
                        ("all:" + " ".join(self.all_assignments[evn, stream])).rjust(
                            20
                        ),
                        end="",
                    )
                else:
                    print(
                        " ".join([str(self.assignments.get(v, v)) for v in val]).rjust(
                            20
                        ),
                        end="",
                    )
            print("")
