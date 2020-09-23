from graph.Element import Element


class Node(Element):
    def __init__(self, kind, id, operator):
        self.kind = kind
        self.id = id
        self.predecessors = {}
        self.successors = {}

        # Temporal
        self.operator = operator

    def add_predecessor(self, id_parent, e):
        self.predecessors[id_parent] = e

    def add_successor(self, id_child, e):
        self.successors[id_child] = e

    def accept(self, visitor, udf, orientation, last_iter):
        visitor.visit_node(self, udf, orientation, last_iter)