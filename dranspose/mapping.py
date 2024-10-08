import uuid
from collections import defaultdict
from typing import List, Dict, Optional, Iterable

from pydantic import validate_call

from dranspose.protocol import (
    WorkAssignment,
    StreamName,
    VirtualWorker,
    WorkerName,
    EventNumber,
    WorkerState,
    VirtualConstraint,
)


class NotYetAssigned(Exception):
    pass


class Mapping:
    @validate_call
    def __init__(
        self,
        m: Dict[StreamName, List[Optional[List[VirtualWorker]]]],
        add_start_end: bool = True,
    ) -> None:
        if len(set(map(len, m.values()))) > 1:
            raise Exception("length not equal: ", list(map(len, m.values())))
        if add_start_end:
            # get all tags:
            tags = set()
            for li in m.values():
                for frame in li:
                    if frame:
                        for vw in frame:
                            tags.update(vw.tags)

            for li in m.values():
                li.insert(0, [VirtualWorker(tags={t}) for t in tags])
                li.append([VirtualWorker(tags={t}) for t in tags])

        # print("mapping is", m)
        self.mapping = m
        self.uuid = uuid.uuid4()
        self.assignments: dict[VirtualConstraint, WorkerName] = {}
        self.all_assignments: dict[
            tuple[EventNumber, StreamName, int], list[WorkerName]
        ] = defaultdict(list)
        self.complete_events = 0

    def len(self) -> int:
        if len(self.mapping) > 0:
            return len(list(self.mapping.values())[0])
        else:
            return 0

    def assign_next(
        self,
        worker: WorkerState,
        all_workers: list[WorkerState],
        completed: Optional[EventNumber] = None,
        horizon: int = 0,
    ) -> list[VirtualWorker]:
        assigned_to: list[VirtualWorker] = []
        maxassign = EventNumber(self.len() + 1)
        still_has_work = False
        if completed is not None:
            for evnint in range(completed + 1, self.complete_events - horizon):
                wa = self.get_event_workers(EventNumber(evnint))
                if worker.name in wa.get_all_workers():
                    still_has_work = True
        if still_has_work:
            return assigned_to
        for evnint in range(self.complete_events, self.len()):
            evn = EventNumber(evnint)
            for stream, v in self.mapping.items():  # first fill the alls
                assign = v[evn]
                if (
                    assign is not None and len(assign) > 0
                ):  # frame exists and is not discarded
                    for i, vw in enumerate(assign):
                        if vw.constraint is None:
                            required_tags = vw.tags
                            all_worker_names_with_required_tags = [
                                ws.name
                                for ws in all_workers
                                if required_tags.issubset(ws.tags)
                            ]
                            if worker.name in (
                                set(all_worker_names_with_required_tags)
                                - set(self.all_assignments[(evn, stream, i)])
                            ):
                                # assign worker to the "all" of the current stream as it is not yet in it
                                self.all_assignments[(evn, stream, i)].append(
                                    worker.name
                                )
                                assigned_to.append(vw)
                                maxassign = evn  # we assigned the worker to this event, don't continue into the future

            for stream, v in self.mapping.items():  # now fill a specific
                assign = v[evn]
                if (
                    assign is not None and len(assign) > 0
                ):  # frame exists and is not discarded
                    for vw in assign:
                        if vw.constraint is not None:  # not all
                            if vw.constraint not in self.assignments:
                                if vw.tags.issubset(worker.tags):
                                    self.assignments[vw.constraint] = worker.name
                                    self.update_filled(all_workers)
                                    assigned_to.append(vw)
                                    return assigned_to
            if evn == maxassign:
                self.update_filled(all_workers)
                return assigned_to
        if assigned_to == []:
            self.update_filled(all_workers)
        return assigned_to

    def min_workers(self) -> int:
        minimum = 0
        for i in zip(*self.mapping.values()):
            workers = set()
            for val in i:
                if val is not None:
                    workers.update(
                        [v.constraint for v in val if v.constraint is not None]
                    )
            minimum = max(minimum, len(workers))
        return minimum

    def get_event_workers(self, no: EventNumber) -> WorkAssignment:
        ret: dict[StreamName, set[WorkerName]] = defaultdict(set)
        if no > self.complete_events - 1:
            raise NotYetAssigned()
        for s, v in self.mapping.items():
            assign = v[no]
            if assign is None:
                continue
            if len(assign) == 0:
                ret[s] = set()
            for i, vw in enumerate(assign):
                if vw.constraint is None:  # get from all
                    ret[s].update(self.all_assignments[(no, s, i)])
                else:
                    ret[s].add(self.assignments[vw.constraint])
        return WorkAssignment(
            event_number=no, assignments={s: sorted(v) for s, v in ret.items()}
        )

    def update_filled(self, all_workers: list[WorkerState]) -> None:
        for evnint in range(self.complete_events, self.len()):
            evn = EventNumber(evnint)
            for stream, v in self.mapping.items():
                assign = v[evn]
                if assign is None:
                    continue
                for i, vw in enumerate(assign):
                    if vw.constraint is None:  # all worker with tags
                        required_tags = vw.tags
                        all_worker_names_with_required_tags = [
                            ws.name
                            for ws in all_workers
                            if required_tags.issubset(ws.tags)
                        ]

                        if set(all_worker_names_with_required_tags) != set(
                            self.all_assignments[(evn, stream, i)]
                        ):
                            return
                    else:
                        if vw.constraint not in self.assignments:
                            return
            self.complete_events = max(0, evn + 1)

    @classmethod
    def from_uniform(cls, streams: Iterable[StreamName], ntriggers: int) -> "Mapping":
        m: dict[StreamName, list[list[VirtualWorker] | None]] = {
            s: [
                [VirtualWorker(constraint=VirtualConstraint(i))]
                for i in range(ntriggers)
            ]
            for s in streams
        }
        return Mapping(m)

    def print(self) -> None:
        print(" " * 5, end="")
        for name in self.mapping:
            print(name.rjust(20), end="")
        print("")
        for evnint in range(self.len()):
            evn = EventNumber(evnint)
            print(str(evn + 1).rjust(5), end="")
            for stream in self.mapping:
                val = self.mapping[stream][evn]
                if val is None:
                    print("None".rjust(20), end="")
                    continue
                elif len(val) == 0:
                    print("discard".rjust(20), end="")
                    continue
                else:
                    s = []
                    for i, vw in enumerate(val):
                        if vw.constraint is None:
                            el = (
                                ",".join(map(str, vw.tags))
                                + ":"
                                + " ".join(self.all_assignments[(evn, stream, i)])
                            )
                        else:
                            el = self.assignments.get(vw.constraint, str(vw.constraint))
                        s.append(el)
                    print((";".join(s)).rjust(20), end="")
            print("")
