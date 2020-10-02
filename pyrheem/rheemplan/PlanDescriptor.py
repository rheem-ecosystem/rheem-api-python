from config.config_reader import get_configurated_boundaries


class PlanDescriptor:

    def __init__(self):
        self.sinks = []
        self.sources = []
        self.boundary_operators = get_configurated_boundaries()

        print("boundary_operators: ", self.boundary_operators)

    def get_boundary_operators(self):
        return self.boundary_operators

    def add_source(self, operator):
        self.sources.append(operator)

    def get_sources(self):
        return self.sources

    def add_sink(self, operator):
        self.sinks.append(operator)

    def get_sinks(self):
        return self.sinks
