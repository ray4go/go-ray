import socket

import ray

import goray


@goray.remote(num_cpus=0)
def chain_call(nodeIds: list[str], currIdx: int, remainStep: int, cross=False) -> map:
    hostname = socket.gethostname()
    print("python task", currIdx, hostname)
    nextNodeId = nodeIds[currIdx % len(nodeIds)]
    currIdx += 1
    hostNames = {}
    if remainStep > 0:
        if cross:
            task = goray.golang_task("ChainCall")
        else:
            task = chain_call
        obj = task.options(resources={f"node:{nextNodeId}": 0.001}, num_cpus=0).remote(
            nodeIds, currIdx, remainStep - 1, cross
        )
        hostNames = ray.get(obj)
    hostNames[hostname] = {}
    return hostNames


@goray.remote
class ChainCallActor:
    def __init__(self, cross: bool, golang_actor_type_name: str):
        self.cross = cross
        self.golang_actor_type_name = golang_actor_type_name

    def ChainCall(self, nodeIds: list[str], currIdx: int, remainStep: int) -> map:
        hostname = socket.gethostname()
        print("python actor", currIdx, hostname)
        nextNodeId = nodeIds[currIdx % len(nodeIds)]
        currIdx += 1
        hostNames = {}
        if remainStep > 0:
            if self.cross:
                cls = goray.golang_actor_class(self.golang_actor_type_name)

            else:
                cls = ChainCallActor
            act = cls.options(
                resources={f"node:{nextNodeId}": 0.001}, num_cpus=0
            ).remote(self.cross, self.golang_actor_type_name)
            hostNames = ray.get(act.ChainCall.remote(nodeIds, currIdx, remainStep - 1))
        hostNames[hostname] = {}
        return hostNames
