import logging
import uuid
from collections import defaultdict
from typing import List, Dict, Optional, Iterable

from pydantic import validate_call, field_validator, BaseModel

from dranspose.protocol import (
    WorkAssignment,
    StreamName,
    VirtualWorker,
    WorkerName,
    EventNumber,
    WorkerState,
    VirtualConstraint,
    MappingName,
    WorkerTag,
)


class NotYetAssigned(Exception):
    pass


class StillHasWork(Exception):
    pass


logger = logging.getLogger(__name__)


class Map(BaseModel):
    mapping: dict[StreamName, list[Optional[list[VirtualWorker]]]]

    @field_validator("mapping")
    @classmethod
    def same_no_evs_stream(
        cls, v: dict[StreamName, list[Optional[list[VirtualWorker]]]]
    ) -> dict[StreamName, list[Optional[list[VirtualWorker]]]]:
        if len(set(map(len, v.values()))) > 1:
            raise ValueError("length not equal: ", list(map(len, v.values())))
        return v

    def no_events(self) -> int:
        if len(self.mapping) > 0:
            return len(next(iter(self.mapping.values())))
        else:
            return 0

    def items(self) -> Iterable[tuple[StreamName, list[Optional[list[VirtualWorker]]]]]:
        return self.mapping.items()

    def streams(self) -> set[StreamName]:
        return set(self.mapping.keys())

    def all_tags(self) -> set[WorkerTag]:
        tags = set()
        for li in self.mapping.values():
            for frame in li:
                if frame is not None:
                    for vw in frame:
                        tags.update(vw.tags)
        return tags

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


class ActiveMap(Map):
    assignments: dict[VirtualConstraint, WorkerName] = {}
    all_assignments: dict[
        tuple[EventNumber, StreamName, int], list[WorkerName]
    ] = defaultdict(list)
    complete_events: int = 0
    start_event: EventNumber

    def assign_next(
        self,
        worker: WorkerState,
        all_workers: list[WorkerState],
        completed: Optional[
            EventNumber
        ] = None,  # last event number just completed by this worker
        horizon: int = 0,
    ) -> list[VirtualWorker]:
        assigned_to: list[
            VirtualWorker
        ] = []  # virtual workers the real worker is assigned to, but used nowhere
        maxassign = EventNumber(self.no_events() + 1)
        still_has_work = False
        if completed is not None:
            for evnint in range(completed + 1, self.complete_events - horizon):
                wa = self.get_event_workers(EventNumber(evnint))
                if worker.name in wa.get_all_workers():
                    still_has_work = True
        if still_has_work:
            raise StillHasWork()  # we will not assign the worker if it is assigned until the horizon
        for evnint in range(self.complete_events, self.no_events()):
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
        for evnint in range(self.complete_events, self.no_events()):
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

    def print(self) -> None:
        print(" " * 5, end="")
        for name in self.mapping:
            print(name.rjust(20), end="")
        print("")
        for evnint in range(self.no_events()):
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


class MappingSequence:
    """
    As the Mapping for many points quickly become large and allocate large amounts of memory,
    we make use of the repetitive structure.
    The parts are a string keyed dictionary of usual maps and
    the offsets are the superstructure on how the parts are combined
    """

    @validate_call
    def __init__(
        self,
        parts: dict[MappingName, dict[StreamName, list[Optional[list[VirtualWorker]]]]],
        sequence: list[MappingName],
        add_start_end: bool = True,
    ) -> None:
        assert (
            set(sequence) - set(parts.keys()) == set()
        ), "all MappingNames in sequence must exist"
        self.parts = {n: Map(mapping=m) for n, m in parts.items()}
        self.all_streams = set()
        for part in self.parts.values():
            self.all_streams.update(part.streams())
        self.all_tags = set()
        for part in self.parts.values():
            self.all_tags.update(part.all_tags())
        if add_start_end:
            start_end_part = Map(
                mapping={
                    stream: [[VirtualWorker(tags={t}) for t in self.all_tags]]
                    for stream in self.all_streams
                }
            )

            self.parts[MappingName("reserved_start_end")] = start_end_part
            sequence.insert(0, MappingName("reserved_start_end"))
            sequence.append(MappingName("reserved_start_end"))

        self.sequence = sequence
        self.uuid = uuid.uuid4()
        self.complete_events = 0

        self.queued_workers = []

        self.current_sequence_index = 0
        first_active = ActiveMap(
            start_event=EventNumber(0),
            mapping=self.parts[self.sequence[self.current_sequence_index]].mapping,
        )
        self.active_maps = [first_active]

    def len(self) -> int:
        return sum((self.parts[seq].no_events() for seq in self.sequence))

    def assign_next(
        self,
        worker: WorkerState,
        all_workers: list[WorkerState],
        completed: Optional[EventNumber] = None,
        horizon: int = 0,
        future: int = 10000,
        fill_from_queue: bool = True,
    ) -> list[VirtualWorker]:
        virt = []

        for map in self.active_maps:
            if map.start_event + map.no_events() < self.complete_events:
                # these maps are already complete
                continue
            try:
                virt = map.assign_next(worker, all_workers, completed, horizon)
                if len(virt) > 0:
                    self.complete_events = map.complete_events + map.start_event
                    logger.debug("assigned to %s in map %s", virt, map)
                    return virt
            except StillHasWork:
                return []
        update_completed = True
        while len(virt) == 0 and len(self.active_maps) < len(self.sequence):
            last_map = self.active_maps[-1]
            next_map = ActiveMap(
                start_event=EventNumber(last_map.start_event + last_map.no_events()),
                mapping=self.parts[self.sequence[len(self.active_maps)]].mapping,
            )
            logger.info(
                "start new active map %s starting at %d",
                self.sequence[len(self.active_maps)],
                next_map.start_event,
            )
            self.active_maps.append(next_map)
            if fill_from_queue:
                still_not_assigned = []
                for w, aw, c in self.queued_workers:
                    logger.info("trying to assign %s", w)
                    ret = self.assign_next(w, aw, c, horizon, fill_from_queue=False)
                    # ret = next_map.assign_next(w,aw,c,horizon)
                    if len(ret) == 0:
                        logger.info("worker %s is still not assignable", w)
                        # queued worker is still not possible
                        still_not_assigned.append((w, aw, c))
                    else:
                        logger.info(
                            "finally assigned worker %s in map starting at %d",
                            w,
                            next_map.start_event,
                        )
                self.queued_workers = still_not_assigned
            try:
                virt = next_map.assign_next(worker, all_workers, completed, horizon)
            except StillHasWork:
                return []
            if last_map.complete_events == last_map.no_events() and update_completed:
                self.complete_events = next_map.complete_events + next_map.start_event
                update_completed = False
            if next_map.start_event > self.complete_events + future and len(virt) == 0:
                logger.info(
                    "the worker %s is not useful in the next %d events, queuing it",
                    worker,
                    future,
                )
                self.queued_workers.append((worker, all_workers, completed))
                break
        logger.debug("assigned to %s", virt)

        return virt

    def get_event_workers(self, no: EventNumber) -> WorkAssignment:
        am = None
        if no > self.active_maps[-1].start_event + self.active_maps[-1].no_events():
            raise NotYetAssigned()
        for map in reversed(self.active_maps):
            if map.start_event <= no:
                am = map
                break
        if am is None:
            raise NotYetAssigned()
        wa = am.get_event_workers(EventNumber(no - am.start_event))
        wa.event_number = no
        logger.debug("got event workers for %d: %s", no, wa)
        return wa

    def print(self) -> None:
        print("currently active:", len(self.active_maps))
        for am in self.active_maps:
            print("Starting at event", am.start_event)
            am.print()

    def expand(self) -> dict[StreamName, list[Optional[list[VirtualWorker]]]]:
        mapping: dict[StreamName, list[Optional[list[VirtualWorker]]]] = {
            s: [] for s in self.all_streams
        }
        for seq in self.sequence:
            part = self.parts[seq]
            padding_len = 0
            for stream, li in part.items():
                mapping[stream] += li
                padding_len = len(li)
            for stream in self.all_streams - set(part.streams()):
                mapping[stream] += [None] * padding_len
        return mapping

    def min_workers(self) -> int:
        return max([p.min_workers() for p in self.parts.values()])


# legacy code which soon can be removed
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
