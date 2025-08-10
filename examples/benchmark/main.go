package main

import (
	"fmt"
	"log"
	"time"

	"github.com/ray4go/go-ray/ray"
)

type tracker struct {
	msg   string
	start time.Time
}

func Tracker(msg string) *tracker {
	return &tracker{msg, time.Now()}
}
func (t *tracker) Print() {
	fmt.Printf("%s 耗时: %v\n", t.msg, time.Since(t.start))
}

func init() {
	//ffi.ZeroCopy = true
	ray.Init(demo{}, map[string]any{"Counter": NewActor}, driver) // 初始化，注册ray driver, ray tasks 和 actors
}

const pycode = `
import ray
import json
nodes = ray.nodes()
print(json.dumps(nodes, indent=4))

target_node = ''
for node in nodes:
	name, cpu, mem = node['NodeName'], node['Resources']['CPU'], node['Resources']['memory']/2**30
	print(f"Node: {name}, CPU: {cpu:.1f}, Memory: {mem:.2f} GB")
	if node['alive'] and node['NodeManagerAddress'] != node['NodeName']:
		target_node = node['NodeName']
write(target_node)
`

func driver() int {
	func() {
		//opt := ray.Option("resources", map[string]float32{fmt.Sprintf("node:%s", nodeId): 0.01})
		opt := ray.Option("num_cpus", 1)
		ray.RemoteCall("Empty", opt).Get0() // warm up
		defer Tracker("ray task").Print()
		for i := 0; i < 5000; i++ {
			objRef := ray.RemoteCall("Empty")
			objRef.Get0()
		}
	}()
	return 0
	{
		nodeId, err := ray.CallPythonCode(pycode)
		if err != nil {
			log.Printf("call python code failed: %v", err)
		}
		//cancel_when_sigint_context()
		fmt.Println("nodeId: ", nodeId)
		if nodeId == "" {
			return 0
		}
	}

}

// raytasks
type demo struct{}

func (_ demo) Empty() {
	//fmt.Println("Empty")
	//time.Sleep(100 * time.Second)
}

func (_ demo) TheAnswerOfWorld() int64 {
	time.Sleep(1 * time.Second)
	return 42
}

func (_ demo) Divide(a, b int64) (int64, int64) {
	return a / b, a % b
}

// ray actor
type counter struct {
	num int
}

func NewActor(n int) *counter {
	return &counter{num: n}
}

func (c *counter) Incr(n int) int {
	fmt.Printf("Incr %d -> %d\n", c.num, c.num+n)
	c.num += n
	return c.num
}

func (c *counter) Decr(n int) int {
	fmt.Printf("Decr %d -> %d\n", c.num, c.num-n)
	c.num -= n
	return c.num
}

// main 函数不会被调用，但不可省略
func main() {}
