from dranspose.event import ResultData


class FluorescenceReducer:
    def __init__(self, parameters=None):
        self.number = 0
        self.publish = {"map": {}}

    def process_result(self, result: ResultData, parameters=None):
        print(result)
        if result.payload:
            self.publish["map"][result.payload["position"]] = result.payload[
                "concentations"
            ]
