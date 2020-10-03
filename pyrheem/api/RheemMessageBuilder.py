import rheemintegration_pb2
import sys

class RheemMessageBuilder:

    def __init__(self, operators):

        plan = rheemintegration_pb2.RheemPlan()
        path = "/Users/rodrigopardomeza/scalytics/rheem-api-python/protobuf/rheemplan"

        try:
            f = open(path, "rb")
            plan.ParseFromString(f.read())
            f.close()
        except IOError:
            print(path + ": Could not open file.  Creating a new one.")

        objs = {}
        for op in operators:
            obj = plan.operators.add()
            obj.id = op.id
            obj.kind = op.kind
            if op.udf is not None:
                obj.udf = op.udf
            #if op.iterator is not None:
            #    obj.iterator = op.iterator

            #self.insert_operator(plan.OperatorSeed.add(), op)

        # Write the new address book back to disk.
        f = open(path, "wb")
        f.write(plan.SerializeToString())
        f.close()
        pass
