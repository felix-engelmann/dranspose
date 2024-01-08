class FluorescenceWorker:
    @staticmethod
    def describe_parameters():
        return [IntParameter(name="roi1-start")]

    def __init__(self, parameters=None):
        self.number = 0

    def process_event(self, event: EventData, parameters=None):
        print(event)
        # parse zmq frames, fit spectra to get concentrations, extract motor position
        return {"position": mot, "concentrations": ...}
