import rheemintegration_pb2
import sys

class RheemMessageBuilder:

    #TODO agregar tipo a UDF
    #TODO Llenar Rheem Context
    #TODO Llenar objeto RheemInput

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

            udf_def = obj.udf
            udf_def.wrapper = op.wrapper
            udf_def.function = op.udf

            objs[op.id] = obj

        for op in operators:
            for pre in op.previous:
                if op is not None and pre is not None:
                    pre_elem = objs[op.id].input_slots.add()
                    pre_elem.owner_id = op.id
                    #occ = pre_elem.occupant.add()
                    occ = pre_elem.occupant
                    #occ.op = objs[pre.id]
                    occ.occupant_id = pre.id

                    suc_elem = objs[pre.id].output_slots.add()
                    suc_elem.owner_id = pre.id
                    occ = suc_elem.occupant
                    occ.occupant_id = op.id

                    pass

        # no aguanta referencias, cambiar por ID
        # enviar map_id_objs

        # Write the new address book back to disk.
        f = open(path, "wb")
        f.write(plan.SerializeToString())
        f.close()
        pass
