import pytest

from dranspose.mapping import Mapping, NotYetAssigned
from dranspose.protocol import (
    VirtualWorker,
    StreamName,
    WorkerName,
    EventNumber,
    WorkAssignment,
    WorkerState,
    VirtualConstraint,
    WorkerTag,
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


def test_discard() -> None:
    ntrig = 10
    m = Mapping(
        {StreamName("test"): [[] for i in range(ntrig)]},
        add_start_end=False,
    )

    m.print()
    all_workers = [
        WorkerState(name=WorkerName("w1")),
        WorkerState(name=WorkerName("w2")),
    ]
    r = m.assign_next(all_workers[0], all_workers)
    print(r)
    r = m.assign_next(all_workers[1], all_workers)
    print(r)

    m.print()
    print("completed", m.complete_events)

    # m.assign_next(all_workers[0], all_workers)

    assign = m.get_event_workers(EventNumber(8))
    print("assign is", assign)
    assert assign.assignments == {}


def test_discard_only_one() -> None:
    ntrig = 10
    m = Mapping(
        {
            StreamName("test"): [[] for i in range(ntrig)],
            StreamName("test2"): [
                [VirtualWorker(constraint=VirtualConstraint(i))] for i in range(ntrig)
            ],
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
    print("completed", m.complete_events)

    with pytest.raises(NotYetAssigned):
        m.get_event_workers(EventNumber(8))

    m.assign_next(all_workers[0], all_workers)

    assign = m.get_event_workers(EventNumber(2))
    assert assign.assignments[StreamName("test2")] == [WorkerName("w1")]


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
    print(evworkers)
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


def test_all_wrap_tags() -> None:
    ntrig = 10
    m = Mapping(
        {
            StreamName("test"): [
                [VirtualWorker(constraint=VirtualConstraint(i))]
                if i % 4
                else [VirtualWorker(tags={WorkerTag("tag"), WorkerTag("bla")})]
                for i in range(ntrig)
            ],
            StreamName("test2"): [
                [VirtualWorker(constraint=VirtualConstraint(i))]
                if i % 4
                else [VirtualWorker(tags={WorkerTag("tag2")})]
                for i in range(ntrig)
            ],
        },
        add_start_end=True,
    )
    print("before assignment")
    m.print()

    first_test = m.mapping[StreamName("test")][0]
    assert first_test is not None
    assert sorted([tuple(t.tags) for t in first_test]) == sorted(
        [("generic",), ("tag",), ("bla",), ("tag2",)]
    )

    all_workers = [
        WorkerState(
            name=WorkerName("w1"),
            tags={WorkerTag("generic"), WorkerTag("tag"), WorkerTag("bla")},
        ),
        WorkerState(
            name=WorkerName("w2"), tags={WorkerTag("tag2"), WorkerTag("generic")}
        ),
    ]
    for i in range(4):
        print(m.complete_events)
        m.assign_next(all_workers[0], all_workers)
        m.assign_next(all_workers[1], all_workers)
        print(m.complete_events)
        print("--")
        # m.assign_next("w3")
    print(m.assignments)
    print(m.all_assignments)
    m.print()

    evworkers = m.get_event_workers(EventNumber(0))
    print(evworkers)
    should = {"test": {"w2", "w1"}, "test2": {"w2", "w1"}}
    for st, wn in evworkers.assignments.items():
        assert set(wn) == should[st]


def test_uniform() -> None:
    m = Mapping.from_uniform({StreamName("orca"), StreamName("panda")}, 10)
    b = m.mapping[StreamName("orca")][10]
    assert b is not None
    a = b[0]
    assert isinstance(a, VirtualWorker)
    assert a.constraint == 9
