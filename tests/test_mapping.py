import pytest

from dranspose.mapping import Mapping, NotYetAssigned
from dranspose.protocol import (
    VirtualWorker,
    StreamName,
    WorkerName,
    EventNumber,
    WorkAssignment,
)


def test_simple_map():
    ntrig = 10
    m = Mapping({StreamName("test"): [[VirtualWorker(i)] for i in range(ntrig)]})

    assert m.len() == ntrig


def test_none():
    ntrig = 10
    m = Mapping(
        {
            StreamName("test"): [
                [VirtualWorker(i)] if i % 4 == 0 else None for i in range(ntrig)
            ]
        }
    )

    m.print()
    all_workers = [WorkerName("w1"), WorkerName("w2")]
    m.assign_next(WorkerName("w1"), all_workers)
    m.assign_next(WorkerName("w2"), all_workers)

    m.print()

    with pytest.raises(NotYetAssigned):
        m.get_event_workers(EventNumber(8))

    m.assign_next(WorkerName("w1"), all_workers)

    assign = m.get_event_workers(EventNumber(8))
    assert assign.assignments[StreamName("test")] == [WorkerName("w1")]


def test_all():
    ntrig = 10
    m = Mapping(
        {
            StreamName("test"): [
                [VirtualWorker(i)] if i % 4 else "all" for i in range(ntrig)
            ]
        }
    )

    m.print()
    all_workers = [WorkerName("w1"), WorkerName("w2")]
    for _ in range(3):
        m.assign_next(WorkerName("w1"), all_workers)
        m.assign_next(WorkerName("w2"), all_workers)

    m.print()

    with pytest.raises(NotYetAssigned):
        m.get_event_workers(EventNumber(4))

    m.assign_next(WorkerName("w1"), all_workers)

    m.print()

    assign = m.get_event_workers(EventNumber(4))
    assert set(assign.assignments[StreamName("test")]) == set(all_workers)


def test_multiple():
    ntrig = 10
    m = Mapping(
        {
            StreamName("eiger"): [[VirtualWorker(2 * i)] for i in range(1, ntrig)],
            StreamName("alba"): [
                [VirtualWorker(2 * i), VirtualWorker(2 * i + 1)]
                for i in range(1, ntrig)
            ],
            StreamName("orca"): [[VirtualWorker(2 * i + 1)] for i in range(1, ntrig)],
        }
    )
    m.print()
    all_workers = [WorkerName("w1"), WorkerName("w2"), WorkerName("w3")]
    for i in range(5):
        print(m.complete_events)
        m.assign_next(WorkerName("w1"), all_workers)
        m.assign_next(WorkerName("w2"), all_workers)
        m.assign_next(WorkerName("w3"), all_workers)
        print(m.complete_events)
        print("--")
        # m.assign_next("w3")
    print(m.assignments)
    print(m.all_assignments)
    assert m.complete_events == 7

    evworkers = m.get_event_workers(EventNumber(m.complete_events - 1))

    assert evworkers == WorkAssignment(
        event_number=6,
        assignments={"eiger": ["w1"], "alba": ["w1", "w2"], "orca": ["w2"]},
    )
    assert m.min_workers() == 2


def test_mixed_all():
    ntrig = 10
    m = Mapping(
        {
            StreamName("eiger"): [[VirtualWorker(2 * i)] for i in range(1, ntrig)],
            StreamName("orca"): [[VirtualWorker(2 * i + 1)] for i in range(1, ntrig)],
            StreamName("announcer"): ["all" for i in range(1, ntrig)],
        }
    )
    m.print()

    all_workers = [WorkerName("w1"), WorkerName("w2"), WorkerName("w3")]
    for i in range(4):
        print(m.complete_events)
        m.assign_next(WorkerName("w1"), all_workers)
        m.assign_next(WorkerName("w2"), all_workers)
        m.assign_next(WorkerName("w3"), all_workers)
        print(m.complete_events)
        print("--")
        # m.assign_next("w3")
    print(m.assignments)
    print(m.all_assignments)
    assert m.all_assignments[(1, "announcer")] == ["w2", "w3", "w1"]

    m.print()
