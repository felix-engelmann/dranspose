import uuid
from typing import List, Dict, Union


class Mapping:
    def __init__(self):
        ntrig = 50000
        self.mapping = {"orca": [[2*i] for i in range(1, ntrig)],"eiger":[[2*i+1] for i in range(1, ntrig)],
                        "alba": [[2*i,2*i+1] for i in range(1, ntrig)],
                        "slow": [[2*i] for i in range(1, ntrig)]}
        self.uuid = uuid.uuid4()
        self.assignments = {}
        self.complete_events = 0

    def len(self) -> int:
        return len(list(self.mapping.values())[0])
    def assign_next(self, worker) -> Union[int|None]:
        for evn in range(self.complete_events, self.len()):
            for v in self.mapping.values():
                for w in v[evn]:
                    if w not in self.assignments:
                        self.assignments[w] = worker
                        self.update_filled()
                        return w

    def get_event_workers(self,no) -> Dict[str, List[str]]:
        ret = {}
        for s,v in self.mapping.items():
            ret[s] = [self.assignments[x] for x in v[no]]
        return ret

    def update_filled(self):
        for evn in range(self.complete_events, self.len()):
            for v in self.mapping.values():
                for w in v[evn]:
                    if w not in self.assignments:
                        self.complete_events = max(0, evn)
                        return
    def print(self):
        for i in self.mapping:
            print(i.rjust(20), end="")
        print("")
        for evn, i in enumerate(zip(*self.mapping.values())):
            print(str(evn+1).rjust(5), end="")
            for val in i:
                print(" ".join([str(self.assignments.get(v,v)) for v in val]).rjust(20), end="")
            print("")


if __name__ == "__main__":
    m = Mapping()

    for i in range(1):
        m.assign_next("w1")
        m.assign_next("w2")
        #m.assign_next("w3")
    print(m.assignments)
    print(m.complete_events)

    print(m.get_event_workers(0))
    m.print()

