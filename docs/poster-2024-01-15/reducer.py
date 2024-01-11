class FluorescenceReducer:
    @staticmethod
    def describe_parameters():
        return [FileParameter(name="dest_file")]

    def __init__(self, parameters=None):
        self.publish = {"map": {}}

    def process_result(self, result: ResultData, parameters=None):
        if result.payload:
            self.publish["map"][result.payload["position"]] = result.payload["concentations"]
