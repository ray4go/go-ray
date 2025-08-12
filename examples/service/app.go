package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ray4go/go-ray/ray"
)

const pythonGetNodesCode = `
import ray

def ray_nodes(exclude_self=False):
    nodes = ray.nodes()
    node_ids = []
    self_id = ray.get_runtime_context().get_node_id()
    for node in nodes:
        if not node["alive"]:
            continue
        if exclude_self and node["NodeID"] == self_id:
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

const version = "v0.1.1"

type raytask struct{}

func (_ raytask) Hello() string {
	hostName, _ := os.Hostname()
	return fmt.Sprintf("[%v] Go hello from %s, version: %s", time.Now(), hostName, version)
}

type actor struct {
	cross         bool
	actorTypeName string
}

func startTasks() {
	var nodeIds []string
	err := ray.CallPythonCode(pythonGetNodesCode).GetInto(&nodeIds)
	if err != nil {
		panic(err)
	}
	fmt.Println("nodeIds", nodeIds)
	for {
		for _, nodeId := range nodeIds {
			fmt.Println(nodeId)
			opt := ray.Option("resources", map[string]float32{fmt.Sprintf("node:%s", nodeId): 0.01})
			opt2 := ray.Option("num_cpus", 0)
			var hello string
			err := ray.RemoteCall("Hello", opt, opt2).GetInto(&hello)
			if err != nil {
				fmt.Printf("error to call Hello in %s: %v\n", nodeId, err)
			} else {
				fmt.Println(hello)
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func newActor() *actor {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("get cwd path failed: %v", err)
	}
	fmt.Println("cwd", cwd)

	go startTasks()

	return &actor{}
}
func (actor *actor) GetVersion() string {
	return version
}

func init() {
	ray.Init(raytask{}, map[string]any{"Actor": newActor}, func() int {
		var oldVersion string
		oldApp, err := ray.GetActor("app", ray.Option("namespace", "app"))
		fmt.Println("oldApp", oldApp, err)
		if err == nil {
			oldApp.RemoteCall("GetVersion").GetInto(&oldVersion, 1)
			err = oldApp.Kill()
			if err != nil {
				panic(err)
			}
		}
		app := ray.NewActor("Actor",
			ray.Option("name", "app"),
			ray.Option("namespace", "app"),
			ray.Option("lifetime", "detached"),
			ray.Option("max_restarts", -1),
		)
		fmt.Println(app.RemoteCall("GetVersion").GetAll())
		fmt.Printf("version %s -> %s", oldVersion, version)
		return 0
	})
}

func main() {}
