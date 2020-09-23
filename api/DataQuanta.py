from api.Operator import Operator
from graph.Graph import Graph

class DataQuanta:

    def __init__(self, operator=None, plan=None):
        self.operator = operator
        self.plan = plan
        if self.operator.is_source():
            self.plan.add_source(self.operator)
        if self.operator.is_sink():
            self.plan.add_sink(self.operator)

    def map(self, udf):
        def func(iterator):
            return map(udf, iterator)

        return DataQuanta(
            Operator(
                kind="map",
                udf=func,
                previous=self.operator,
                sink=True,
                boundary_operators=self.plan.get_boundary_operators()
            ),
            plan=self.plan
        )

    def filter(self, udf):
        def func(iterator):
            return filter(udf, iterator)

        return DataQuanta(
            Operator(
                kind="filter",
                udf=func,
                previous=self.operator,
                boundary_operators=self.plan.get_boundary_operators()
            ),
            plan=self.plan
        )

    def join(self, key_udf, other, other_key_udf):
        self

    def console(self, end="\n"):
        def consume(iterator):
            for x in iterator:
                print(x, end=end)

        self.__run(consume)

    def __run(self, consumer):
        consumer(self.operator.getIterator())

    def create_graph(self):
        graph = Graph()
        graph.create(self.plan.sinks)
        return graph