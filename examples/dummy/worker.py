import json

from dranspose.event import EventData

import numpy as np


class FluorescenceWorker:
    def __init__(self):
        self.number = 0

    def process_event(self, event: EventData, parameters=None):
        print(event)

        return {"position": 1}
