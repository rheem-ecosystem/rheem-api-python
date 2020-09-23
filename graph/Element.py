import abc


class Element(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def accept(self, visitor, udf, orientation, last_iter):
        pass
