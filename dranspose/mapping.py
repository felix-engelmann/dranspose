import uuid


class Mapping:
    def __init__(self):
        self.mapping = {"detector": [[-2], [-1]], "orca": [[-2], [2, 3], [100, 104]]}

        self.uuid = uuid.uuid4()
