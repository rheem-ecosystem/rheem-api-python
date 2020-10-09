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
                #sink=True,
                boundary_operators=self.plan.get_boundary_operators(),
                wrapper="transform"
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
                #sink=True,
                boundary_operators=self.plan.get_boundary_operators(),
                wrapper="predicate"
            ),
            plan=self.plan
        )

    def sink(self, path, end="\n"):
        def consume(iterator):
            with open(path, 'w') as f:
                for x in iterator:
                    f.write(str(x) + end)

        def func(iterator):
            return self.__run(consume)
        #self.__run(consume)

        return DataQuanta(
            Operator(
                kind="sink",
                udf=func,
                previous=self.operator,
                sink=True,
                boundary_operators=self.plan.get_boundary_operators(),
                wrapper="URL,end"
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

    def execute(self):
        print(self.operator.previous[0].kind)
        self.operator.udf(self.operator.previous[0].getIterator())

    def create_graph(self):
        graph = Graph()
        # what happens when the plan does not have any sink operator
        graph.create(self.plan.sinks)
        return graph