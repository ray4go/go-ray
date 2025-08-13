package cases

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"

	"github.com/ray4go/go-ray/ray"
)

const pythonGetNodesCode = `
import ray

def ray_nodes():
    nodes = ray.nodes()
    node_ids = []
    for node in nodes:
        if not node["alive"]:
            continue
        name, cpu, mem = (
            node["NodeName"],
            node["Resources"].get("CPU", -1),
            node["Resources"].get("memory", -1) / 2**30,
        )
        print(f"Node: {name}, CPU: {cpu:.1f}, Memory: {mem:.2f} GB", node["NodeManagerAddress"])
        node_ids.append(node["NodeManagerAddress"])
    return list(set(node_ids))
`

// 返回剩余 step 后的 hostname 列表
func (_ testTask) ChainCall(nodeIds []string, currIdx int, remainStep int, cross bool) map[string]struct{} {
	hostName, _ := os.Hostname()
	fmt.Println("Go task", currIdx, hostName)
	nextNodeId := nodeIds[currIdx%len(nodeIds)]
	currIdx++

	var hostNames = map[string]struct{}{}
	if remainStep > 0 {
		opt := ray.Option("resources", map[string]float32{fmt.Sprintf("node:%s", nextNodeId): 0.01})
		opt2 := ray.Option("num_cpus", 0)
		var err error
		if cross {
			err = ray.RemoteCallPyTask("chain_call", nodeIds, currIdx, remainStep-1, cross, opt, opt2).GetInto(&hostNames)
		} else {
			err = ray.RemoteCall("ChainCall", nodeIds, currIdx, remainStep-1, cross, opt, opt2).GetInto(&hostNames)
		}
		if err != nil {
			panic(err)
		}
	}
	hostNames[hostName] = struct{}{}
	return hostNames
}

type actor struct {
	cross         bool
	actorTypeName string
}

func newActor(cross bool, actorTypeName string) *actor {
	return &actor{cross, actorTypeName}
}
func (actor *actor) ChainCall(nodeIds []string, currIdx int, remainStep int) map[string]struct{} {
	hostName, _ := os.Hostname()
	fmt.Println("Go actor", currIdx, hostName)
	nextNodeId := nodeIds[currIdx%len(nodeIds)]
	currIdx++

	var hostNames = map[string]struct{}{}
	if remainStep > 0 {
		opt := ray.Option("resources", map[string]float32{fmt.Sprintf("node:%s", nextNodeId): 0.01})
		opt2 := ray.Option("num_cpus", 0)
		var act *ray.ActorHandle
		if actor.cross {
			act = ray.NewPyActor("ChainCallActor", actor.cross, actor.actorTypeName, opt, opt2)
		} else {
			act = ray.NewActor(actor.actorTypeName, actor.cross, actor.actorTypeName, opt, opt2)
		}
		err := act.RemoteCall("ChainCall", nodeIds, currIdx, remainStep-1).GetInto(&hostNames)
		if err != nil {
			panic(err)
		}
	}
	hostNames[hostName] = struct{}{}
	return hostNames
}

func init() {
	actorName := RegisterActor(newActor)

	AddTestCase("ChainCall", func(assert *require.Assertions) {
		var nodeIds []string
		err := ray.CallPythonCode(pythonGetNodesCode).GetInto(&nodeIds)
		assert.NoError(err)
		{
			var hostNames map[string]struct{}
			err := ray.RemoteCall("ChainCall", nodeIds, 0, 6, true).GetInto(&hostNames)
			assert.NoError(err)
			assert.Equal(len(nodeIds), len(hostNames))
		}
		{
			hostNames := newActor(true, actorName).ChainCall(nodeIds, 0, 6)
			assert.Equal(len(nodeIds), len(hostNames))
		}

		{
			fut := ray.NewPyActor("ChainCallActor", false, actorName).RemoteCall("ChainCall", nodeIds, 0, 6)
			var hostNames map[string]struct{}
			err := fut.GetInto(&hostNames)
			assert.NoError(err)
			assert.Equal(len(nodeIds), len(hostNames))
		}
		{
			var hostNames map[string]struct{}
			err := ray.RemoteCall("ChainCall", nodeIds, 0, 6, false).GetInto(&hostNames)
			assert.NoError(err)
			assert.Equal(len(nodeIds), len(hostNames))
		}
		{
			hostNames := newActor(false, actorName).ChainCall(nodeIds, 0, 6)
			assert.Equal(len(nodeIds), len(hostNames))
		}
		{
			var hostNames map[string]struct{}
			err := ray.RemoteCallPyTask("chain_call", nodeIds, 0, 6, false).GetInto(&hostNames)
			assert.NoError(err)
			assert.Equal(len(nodeIds), len(hostNames))
		}
	})
}
