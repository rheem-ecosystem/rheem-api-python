from graph.Node import Node


class Graph:
    def __init__(self):
        self.graph = {}
        self.nodes_no = 0
        self.nodes = []

    def create(self, sinks):
        for sink in iter(sinks):
            self.process_operator(sink)

    def process_operator(self, operator):
        self.add_node(operator.kind, operator.id, operator)

        if len(operator.previous) > 0:
            for parent in operator.previous:
                if parent:
                    self.add_node(parent.kind, parent.id, parent)
                    self.add_link(operator.id, parent.id, 1)
                    self.process_operator(parent)

    def add_node(self, name, id, operator):
        if id in self.nodes:
            return

        self.nodes_no += 1
        self.nodes.append(id)
        new_node = Node(name, id, operator)

        self.graph[id] = new_node

    def add_link(self, id_child, id_parent, e):
        #print("id_child: ", id_child)
        #print("id_parent: ", id_parent)
        if id_child in self.nodes:
            #print("existe hijo")
            if id_parent in self.nodes:
                #print("existe padre")
                self.graph[id_child].add_predecessor(id_parent, e)
                self.graph[id_parent].add_successor(id_child, e)

    def print_adjlist(self):

        for key in self.graph:
            print("Node: ", self.graph[key].kind, " - ", key)
            for key2 in self.graph[key].predecessors:
                print("Papi: ", self.graph[key2].kind, " - ", self.graph[key].predecessors[key2], " - ", key2)
            for key2 in self.graph[key].successors:
                print("Hijo: ", self.graph[key2].kind, " - ", self.graph[key].successors[key2], " - ", key2)

    def get_node(self, id):
        #print("looking for id: ", id)
        return self.graph[id]