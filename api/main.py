from api.DataQuantaBuilder import DataQuantaBuilder
from rheemplan.PlanDescriptor import PlanDescriptor
from graph.Transversal import Transversal
import collections

if __name__ == '__main__':

    # Plan will contain general info about the Rheem Plan created here
    plan = PlanDescriptor()

    # We need to save somewhere the sinks and the sources of the plan
    rheem = DataQuantaBuilder(plan)

    graph = rheem.source([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) \
            .filter(lambda x: x % 2 == 0) \
            .map(lambda y: y * 2) \
            .create_graph()
            #.console()

    print(plan.boundary_operators)
    print("sources")
    for s in plan.sources:
        print(s.kind)
    print("sinks")
    for s in plan.sinks:
        print(s.kind)

    def d(x, y, z):
        if not y:
            y = 1
        else:
            y += 1
        print("UDF: iteration ", y, " in ", x.kind)
        return y

    def define_pipelines(node1, current_pipeline, collection):

        def store_unique(pipe_to_insert):
            for pipe in collection:
                if equivalent_lists(pipe, pipe_to_insert):
                    return
            collection.append(pipe_to_insert)

        def equivalent_lists(l1, l2):
            if collections.Counter(l1) == collections.Counter(l2):
                return True
            else:
                return False

        print("printing collection")
        for i in collection:
            if not i:
                print("vacio")
            else:
                for j in i:
                    print("elem ", j.kind)

        if not current_pipeline:
            current_pipeline = [node1]
            print("current case 1: ", current_pipeline)
        elif node1.operator.is_boundary:
            store_unique(current_pipeline.copy())
            current_pipeline.clear()
            current_pipeline.append(node1)
            print("current case 2: ", current_pipeline)
        else:
            current_pipeline.append(node1)
            print("current case 3: ", current_pipeline)

        if node1.operator.sink:
            store_unique(current_pipeline.copy())
            current_pipeline.clear()
        print("concatenating: ", node1.operator.kind, " ", len(current_pipeline))
        return current_pipeline

    # Works over the graph
    trans = Transversal(
        graph=graph,
        origin=plan.sources,
        # udf=lambda x, y, z: d(x, y, z)
        # UDF always will receive:
        # x: a Node object,
        # y: an object representing the result of the last iteration,
        # z: a collection to store final results inside your UDF
        udf=lambda x, y, z: define_pipelines(x, y, z)
    )

    collected = trans.get_collected_data()

    print("col: ", len(collected))
    for pipe in collected:
        print("separador")
        for node in pipe:
            print(node.kind)
