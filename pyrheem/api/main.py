from api.DataQuantaBuilder import DataQuantaBuilder
from api.Operator import Operator       #quitar, mover
from rheemplan.PlanDescriptor import PlanDescriptor
from graph.Transversal import Transversal
from api.RheemMessageBuilder import RheemMessageBuilder     #quitar, mover
import collections
import cloudpickle
import pickle


def pipeline_func(fun1, fun2):
    def execute(iterator):
       return fun1(fun2(iterator))
    return execute

def separate_stages(collected):
    stages = []
    for pipe in collected:
        print("separador")
        last = None
        wrapper = ""
        for node in reversed(pipe):
            if node.operator.udf is not None:
                if node.operator.is_sink():
                    print(node.id, "Ignoring", node.operator.udf)
                    last = node.operator.udf
                    wrapper = node.operator.wrapper
                elif last is not None:
                    print(node.id, "getting serialized udf", node.operator.udf)
                    last = pipeline_func(last, node.operator.udf)
                    wrapper += "," + node.operator.wrapper
                else:
                    print(node.id, "getting serialized udf", node.operator.udf)
                    last = node.operator.udf
                    wrapper = node.operator.wrapper
        # At this point, last is the cncatenation of every operator in the pipe
        print(last)

        stages.append(cloudpickle.dumps(last))
    i = 0
    for s in stages:
        i += 1
        func = pickle.loads(s)
        for i in func([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]):
            print(i)

    print("col: ", len(collected))
    for pipe in collected:
        print("separador")
        for node in pipe:
            print(node.id)

    return stages

def compress_stages(collected, plan):

    last = None
    sources = []
    sinks = []
    wrapper = ""
    for pipe in collected:
        print("separador")
        last = None
        wrapper = ""
        sources = []
        sinks = []
        for node in reversed(pipe):
            if node.operator.is_source():
                print(node.id, "Ignoring", node.operator.udf)
                sources.append(node.operator)
            elif node.operator.udf is not None:
                if node.operator.is_sink():
                    print(node.id, "Ignoring", node.operator.udf)
                    sinks.append(node.operator)
                    pass
                elif last is not None:
                    print(node.id, "getting serialized udf", node.operator.udf)
                    last = pipeline_func(last, node.operator.udf)
                    wrapper += "|" + node.operator.wrapper
                else:
                    print(node.id, "getting serialized udf", node.operator.udf)
                    last = node.operator.udf
                    wrapper = node.operator.wrapper
                pass
        # At this point, last is the cncatenation of every operator in the pipe
        print("last", last)
    print("last last", last)
    source = None
    sink = None
    for x in sources:
        print("source", x.kind)
        source = Operator(
            kind="source",
            udf=cloudpickle.dumps(x.udf),
            previous=None,
            boundary_operators=plan.get_boundary_operators(),
            wrapper=x.wrapper
        )
    op_pipe = Operator(
        kind="composite",
        udf=cloudpickle.dumps(last),
        previous=source,
        boundary_operators=plan.get_boundary_operators(),
        wrapper=wrapper
    )
    for x in sinks:
        print("sink", x.kind)
        sink = Operator(
            kind="sink",
            udf=cloudpickle.dumps(x.udf),
            previous=op_pipe,
            boundary_operators=plan.get_boundary_operators(),
            sink=True,
            wrapper=x.wrapper
        )

    source.set_successor(op_pipe)
    op_pipe.set_predecessor(source)
    op_pipe.set_successor(sink)
    sink.set_predecessor(op_pipe)

    operators = [source, op_pipe, sink]

    rmb = RheemMessageBuilder(operators)
    pass


if __name__ == '__main__':

    use_composite = 2

    # Plan will contain general info about the Rheem Plan created here
    plan = PlanDescriptor()

    # We need to save somewhere the sinks and the sources of the plan
    rheem = DataQuantaBuilder(plan)

    op2 = """graph = rheem.source([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) \
        .filter(lambda x: x % 2 == 0) \
        .map(lambda y: y * 2) \
        .sink(path="/Users/rodrigopardomeza/PycharmProjects/pyrheem/results/" + "sinktest.txt", end="\n") \
        .create_graph()"""
        #.execute()
        #.console()

    """graph = rheem.source("/Users/rodrigopardomeza/PycharmProjects/rheem-python/python-api/Operator.py") \
        .filter(lambda s: "class" in s) \
        .map(lambda s: "papurri " + s) \
        .sink(path="/Users/rodrigopardomeza/scalytics/rheem-api-python/pyrheem/localresults/localtest.txt", end="") \""""
        #.create_graph()
        #.execute()

    #graph2 = rheem.source("pepito").map(lambda y: y * 2).sink("peputo").create_graph()

    graph3 = rheem.source("pepito").filter(lambda y: y == y.lower()).sink("peputo").create_graph()


    if use_composite == 1:
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

            if not current_pipeline:
                current_pipeline = [node1]

            elif node1.operator.is_boundary:
                store_unique(current_pipeline.copy())
                current_pipeline.clear()
                current_pipeline.append(node1)

            else:
                current_pipeline.append(node1)

            if node1.operator.sink:
                store_unique(current_pipeline.copy())
                current_pipeline.clear()

            return current_pipeline

        # Works over the graph
        """trans = Transversal(
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

        compress_stages(collected, plan)
"""
    elif use_composite == 2:
        # Works over the graph

        def plan_to_list(node, current_list, collection):
            if node.operator not in collection:
                node.operator.serialize_udf()
                collection.append(node.operator)
            return None

        simple_list = Transversal(
            graph=graph3,
            origin=plan.sources,
            # udf=lambda x, y, z: d(x, y, z)
            # UDF always will receive:
            # x: a Node object,
            # y: an object representing the result of the last iteration,
            # z: a collection to store final results inside your UDF
            udf=lambda x, y, z: plan_to_list(x, y, z)
        )

        list = simple_list.get_collected_data()

        print("simple_list")
        for i in list:
            print(i.kind, i.successor)

        RheemMessageBuilder(list)