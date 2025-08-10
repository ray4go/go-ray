package cases

import (
	"github.com/ray4go/go-ray/ray"
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
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

func init() {
	AddTestCase("ChainCallGo2GO", func(assert *require.Assertions) {
		var nodeIds []string
		err := ray.CallPythonCode(pythonGetNodesCode).GetInto(&nodeIds)
		assert.NoError(err)
		assert.NotEmpty(nodeIds)
	})
}
