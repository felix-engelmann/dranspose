import pytest

from dranspose.mapping import Mapping, NotYetAssigned
from dranspose.protocol import (
    VirtualWorker,
    StreamName,
    WorkerName,
    EventNumber,
    WorkAssignment,
    WorkerState,
    WorkerTag,
    VirtualConstraint,
    GENERIC_WORKER,
)


def test_simple_map() -> None:
    ntrig = 10
    m = Mapping(
        {
            StreamName("test"): [
                [VirtualWorker(constraint=VirtualConstraint(i))] for i in range(ntrig)
            ]
        },
        add_start_end=False,
    )

    assert m.len() == ntrig


def test_none() -> None:
    ntrig = 10
    m = Mapping(
        {
            StreamName("test"): [
                [VirtualWorker(constraint=VirtualConstraint(i))] if i % 4 == 0 else None
                for i in range(ntrig)
            ]
        },
        add_start_end=False,
    )

    m.print()
    all_workers = [
        WorkerState(name=WorkerName("w1")),
        WorkerState(name=WorkerName("w2")),
    ]
    m.assign_next(all_workers[0], all_workers)
    m.assign_next(all_workers[1], all_workers)

    m.print()

    with pytest.raises(NotYetAssigned):
        m.get_event_workers(EventNumber(8))

    m.assign_next(all_workers[0], all_workers)

    assign = m.get_event_workers(EventNumber(8))
    assert assign.assignments[StreamName("test")] == [WorkerName("w1")]


def test_auto() -> None:
    ntrig = 10
    m = Mapping(
        {
            StreamName("test"): [
                [VirtualWorker(constraint=VirtualConstraint(i))] if i % 4 == 0 else None
                for i in range(ntrig)
            ]
        },
    )

    m.print()
    all_workers = [
        WorkerState(name=WorkerName("w1")),
        WorkerState(name=WorkerName("w2")),
    ]
    m.assign_next(all_workers[0], all_workers)
    m.assign_next(all_workers[1], all_workers)

    m.print()

    assert 1 == m.complete_events

    with pytest.raises(NotYetAssigned):
        m.get_event_workers(EventNumber(8))


def test_all() -> None:
    ntrig = 10
    m = Mapping(
        {
            StreamName("test"): [
                [VirtualWorker(constraint=VirtualConstraint(i))]
                if i % 4
                else [VirtualWorker()]
                for i in range(ntrig)
            ]
        },
        add_start_end=False,
    )
    print("before assignment")
    m.print()
    all_workers = [
        WorkerState(name=WorkerName("w1")),
        WorkerState(name=WorkerName("w2")),
    ]
    for _ in range(3):
        m.assign_next(all_workers[0], all_workers)
        m.assign_next(all_workers[1], all_workers)

    print("assigned 3x w1,w2")
    m.print()
    print("assignments", m.assignments, m.all_assignments)

    with pytest.raises(NotYetAssigned):
        m.get_event_workers(EventNumber(4))

    m.assign_next(all_workers[0], all_workers)

    print("assigned another w1")
    m.print()

    assign = m.get_event_workers(EventNumber(4))
    assert set(assign.assignments[StreamName("test")]) == set(
        [ws.name for ws in all_workers]
    )


def test_multi_all() -> None:
    ntrig = 10
    m = Mapping(
        {
            StreamName("test"): [
                [VirtualWorker(constraint=VirtualConstraint(i))]
                if i % 4
                else [VirtualWorker()]
                for i in range(ntrig)
            ],
            StreamName("test2"): [
                [VirtualWorker(constraint=VirtualConstraint(i))]
                if i % 4
                else [VirtualWorker()]
                for i in range(ntrig)
            ],
            StreamName("noall"): [
                [VirtualWorker(constraint=VirtualConstraint(i))] for i in range(ntrig)
            ],
        },
        add_start_end=False,
    )

    m.print()
    all_workers = [
        WorkerState(name=WorkerName("w1")),
        WorkerState(name=WorkerName("w2")),
        WorkerState(name=WorkerName("w3")),
    ]
    for _ in range(3):
        m.assign_next(all_workers[0], all_workers)
        m.assign_next(all_workers[1], all_workers)
        m.assign_next(all_workers[2], all_workers)

    m.print()

    m.assign_next(all_workers[0], all_workers)

    m.print()

    assign = m.get_event_workers(EventNumber(4))
    assert set(assign.assignments[StreamName("test")]) == set(
        [ws.name for ws in all_workers]
    )


def test_multiple() -> None:
    ntrig = 10
    m = Mapping(
        {
            StreamName("eiger"): [
                [VirtualWorker(constraint=VirtualConstraint(2 * i))]
                for i in range(1, ntrig)
            ],
            StreamName("alba"): [
                [
                    VirtualWorker(constraint=VirtualConstraint(2 * i)),
                    VirtualWorker(constraint=VirtualConstraint(2 * i + 1)),
                ]
                for i in range(1, ntrig)
            ],
            StreamName("orca"): [
                [VirtualWorker(constraint=VirtualConstraint(2 * i + 1))]
                for i in range(1, ntrig)
            ],
        },
        add_start_end=False,
    )
    m.print()
    all_workers = [
        WorkerState(name=WorkerName("w1")),
        WorkerState(name=WorkerName("w2")),
        WorkerState(name=WorkerName("w3")),
    ]
    for i in range(5):
        print(m.complete_events)
        m.assign_next(all_workers[0], all_workers)
        m.assign_next(all_workers[1], all_workers)
        m.assign_next(all_workers[2], all_workers)
        print(m.complete_events)
        print("--")
        # m.assign_next("w3")
    print(m.assignments)
    print(m.all_assignments)
    assert m.complete_events == 7

    evworkers = m.get_event_workers(EventNumber(m.complete_events - 1))

    assert evworkers == WorkAssignment(
        event_number=EventNumber(6),
        assignments={
            StreamName("eiger"): [WorkerName("w1")],
            StreamName("alba"): [WorkerName("w1"), WorkerName("w2")],
            StreamName("orca"): [WorkerName("w2")],
        },
    )
    assert m.min_workers() == 2


def test_mixed_all() -> None:
    ntrig = 10
    m = Mapping(
        {
            StreamName("eiger"): [
                [VirtualWorker(constraint=VirtualConstraint(2 * i))]
                for i in range(1, ntrig)
            ],
            StreamName("orca"): [
                [VirtualWorker(constraint=VirtualConstraint(2 * i + 1))]
                for i in range(1, ntrig)
            ],
            StreamName("announcer"): [[VirtualWorker()] for i in range(1, ntrig)],
        },
        add_start_end=False,
    )
    m.print()

    all_workers = [
        WorkerState(name=WorkerName("w1")),
        WorkerState(name=WorkerName("w2")),
        WorkerState(name=WorkerName("w3")),
    ]
    for i in range(4):
        print(m.complete_events)
        m.assign_next(all_workers[0], all_workers)
        m.assign_next(all_workers[1], all_workers)
        m.assign_next(all_workers[2], all_workers)
        print(m.complete_events)
        print("--")
        # m.assign_next("w3")
    print(m.assignments)
    print(m.all_assignments)
    m.print()
    assert m.all_assignments[(EventNumber(1), StreamName("announcer"), 0)] == [
        "w1",
        "w2",
        "w3",
    ]

    m.print()
