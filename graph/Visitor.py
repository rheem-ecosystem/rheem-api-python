import abc


class Visitor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def visit_node(self, node, udf, orientation, last_iter):
        pass