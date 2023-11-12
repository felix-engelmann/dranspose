import uuid
from typing import List, Dict, Union


class Mapping:
    def __init__(self):
        ntrig = 10
        self.mapping = {
            # "orca": [[2 * i] for i in range(1, ntrig)],
            "eiger": [[2 * i + 1] for i in range(1, ntrig)],
            # "slow": [[1000+2 * i] if i%3 == 0 else None for i in range(1, ntrig)],
            # "alba": [[4000+2 * i, 2 * i + 1] for i in range(1, ntrig)],
        }
        self.uuid = uuid.uuid4()
        self.assignments = {}
        self.complete_events = 0

    def len(self) -> int:
        return len(list(self.mapping.values())[0])

    def assign_next(self, worker) -> Union[int, None]:
        for evn in range(self.complete_events, self.len()):
            for v in self.mapping.values():
                if v[evn] is None:
                    continue
                for w in v[evn]:
                    if w not in self.assignments:
                        self.assignments[w] = worker
                        self.update_filled()
                        return w
        return None

    def min_workers(self):
        minimum = 0
        for evn, i in enumerate(zip(*self.mapping.values())):
            workers = set()
            for val in i:
                if val is not None:
                    workers.update(val)
            minimum = max(minimum, len(workers))
        return minimum

    def get_event_workers(self, no) -> Dict[str, List[str]]:
        ret = {}
        for s, v in self.mapping.items():
            if v[no] is None:
                continue
            ret[s] = [self.assignments[x] for x in v[no]]
        return ret

    def update_filled(self):
        for evn in range(self.complete_events, self.len()):
            complete = True
            for v in self.mapping.values():
                if v[evn] is None:
                    continue
                for w in v[evn]:
                    if w not in self.assignments:
                        complete = False
                        return
            if complete:
                self.complete_events = max(0, evn + 1)

    def print(self):
        print(" " * 5, end="")
        for i in self.mapping:
            print(i.rjust(20), end="")
        print("")
        for evn, i in enumerate(zip(*self.mapping.values())):
            print(str(evn + 1).rjust(5), end="")
            for val in i:
                if val is None:
                    print("None".rjust(20), end="")
                    continue
                print(
                    " ".join([str(self.assignments.get(v, v)) for v in val]).rjust(20),
                    end="",
                )
            print("")


if __name__ == "__main__":
    m = Mapping()
    m.print()
    for i in range(5):
        print(m.complete_events)
        m.assign_next("w1")
        m.assign_next("w2")
        print(m.complete_events)
        print("--")
        # m.assign_next("w3")
    print(m.assignments)
    print(m.complete_events)

    print(m.get_event_workers(0))
    m.print()

    print(m.min_workers())
