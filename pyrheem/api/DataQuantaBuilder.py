from api.Operator import Operator
from api.DataQuanta import DataQuanta

class DataQuantaBuilder:

    def __init__(self, plan):
        self.plan = plan

    def source(self, source):

        #Uncomment to execute here directly
        #if type(source) is str:
        #    source_ori = open(source, "r")
        #else:
        source_ori = source
        return DataQuanta(
            Operator(
                kind="source",
                udf=source,
                iterator=iter(source_ori),
                boundary_operators=self.plan.get_boundary_operators(),
                wrapper="URL"
            ),
            self.plan
        )
