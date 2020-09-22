from api.DataQuantaBuilder import DataQuantaBuilder
from rheemplan.PlanDescriptor import PlanDescriptor

if __name__ == '__main__':

    # Plan will contain general info about the Rheem Plan created here
    plan = PlanDescriptor()

    # We need to save somewhere the sinks and the sources of the plan
    rheem = DataQuantaBuilder(plan)

    graph = rheem.source([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) \
            .filter(lambda x: x % 2 == 0) \
            .map(lambda y: y * 2) \
            .console()

    print(plan.boundary_operators)
    print("sources")
    for s in plan.sources:
        print(s.kind)
    print("sinks")
    for s in plan.sinks:
        print(s.kind)
