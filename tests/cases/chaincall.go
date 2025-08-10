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
    return node_ids
`

// 返回剩余 step 后的 hostname 列表
func (_ testTask) ChainCall(nodeIds []string, currIdx int, remainStep int) map[string]struct{} {
	hostName, _ := os.Hostname()
	fmt.Println(currIdx, hostName)
	nextNodeId := nodeIds[currIdx%len(nodeIds)]
	currIdx++

	var hostNames = map[string]struct{}{}
	if remainStep > 0 {
		opt := ray.Option("resources", map[string]float32{fmt.Sprintf("node:%s", nextNodeId): 0.01})
		err := ray.RemoteCall("ChainCall", nodeIds, currIdx, remainStep-1, opt).GetInto(&hostNames)
		if err != nil {
			panic(err)
		}
	}
	hostNames[hostName] = struct{}{}
	return hostNames
}

type actor struct{}

func newActor() *actor {
	return &actor{}
}
func (actor *actor) ChainCall(actorName string, nodeIds []string, currIdx int, remainStep int) map[string]struct{} {
	hostName, _ := os.Hostname()
	fmt.Println(currIdx, hostName)
	nextNodeId := nodeIds[currIdx%len(nodeIds)]
	currIdx++

	var hostNames = map[string]struct{}{}
	if remainStep > 0 {
		opt := ray.Option("resources", map[string]float32{fmt.Sprintf("node:%s", nextNodeId): 0.01})
		opt2 := ray.Option("num_cpus", 0)
		act := ray.NewActor(actorName, opt, opt2)
		err := act.RemoteCall("ChainCall", actorName, nodeIds, currIdx, remainStep-1).GetInto(&hostNames)
		if err != nil {
			panic(err)
		}
	}
	hostNames[hostName] = struct{}{}
	return hostNames
}

func init() {
	actorNmae := RegisterActor(newActor)

	AddTestCase("ChainCallGo2GO", func(assert *require.Assertions) {
		var nodeIds []string
		err := ray.CallPythonCode(pythonGetNodesCode).GetInto(&nodeIds)
		assert.NoError(err)
		{
			var hostNames map[string]struct{}
			err := ray.RemoteCall("ChainCall", nodeIds, 0, 6).GetInto(&hostNames)
			assert.NoError(err)
			assert.Equal(len(nodeIds), len(hostNames))
		}
		{
			hostNames := newActor().ChainCall(actorNmae, nodeIds, 0, 6)
			assert.Equal(len(nodeIds), len(hostNames))
		}
	})
}
