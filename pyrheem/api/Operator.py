
class Operator:

    def __init__(self, kind="kind", udf=None, previous=None, iterator=None, sink=False, boundary_operators=None):
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

        print(str(self.getID()) + " " + self.kind, ", is boundary: ", self.is_boundary, ", is source: ",
        self.source, ", is sink: ", self.sink)

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