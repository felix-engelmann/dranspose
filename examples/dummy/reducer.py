from dranspose.event import ResultData


class FluorescenceReducer:
    def __init__(self):
        self.number = 0
        self.publish = {"results": []}

    def process_result(self, result: ResultData, parameters=None):
        print(result)

        self.publish["results"].append(result)
