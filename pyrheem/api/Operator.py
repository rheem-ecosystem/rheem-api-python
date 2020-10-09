import cloudpickle

class Operator:

    # agregar slot e input output slots
    # el protocolbuffer trabajara sobre esta clase para producir un operator_seed basado en operator_base!

    def __init__(self, kind="kind", udf=None, previous=None, iterator=None,
                 sink=False, boundary_operators=None, wrapper=None):
        self.kind = kind
        self.previous = []
        self.previous.append(previous)
        self.udf = udf
        self.sink = sink
        self.id = id(self)
        if self.kind in boundary_operators:
            self.is_boundary = True
        else:
            self.is_boundary = False
        if iterator is None:
            self.source = False
        else:
            self.source = True
        self.iterator = iterator

        self.wrapper = wrapper

        print(str(self.getID()) + " " + self.kind, ", is boundary: ", self.is_boundary, ", is source: ",
        self.source, ", is sink: ", self.sink, " wrapper: ", self.wrapper)

        self.successor = []
        self.predecessor = []

        # should be like this?
        if self.previous:
            for prev in self.previous:
                if prev is not None:
                    prev.set_successor(self)
                    self.set_predecessor(prev)

        if self.is_sink():
            self.successor = []

    def is_source(self):
        return self.source

    def is_sink(self):
        return self.sink

    def getIterator(self):
        # print("Recursion " + str(self.getID()) + " " + self.kind)
        if self.is_source():
            return self.iterator
        return self.udf(self.previous[0].getIterator())

    def getID(self):
        return self.id

    def set_successor(self, suc):
        if self.successor.count(suc) == 0:
            self.successor.append(suc)

    def set_predecessor(self, suc):
        if self.predecessor.count(suc) == 0:
            self.predecessor.append(suc)

    def serialize_udf(self):
        self.udf = cloudpickle.dumps(self.udf)