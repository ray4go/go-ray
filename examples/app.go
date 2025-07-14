package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ray4go/go-ray/ray"
)

func assert(a, b any) {
	if a != b {
		log.Panicf("assert failed: %#v != %#v", a, b)
	}
}

type Point struct {
	X, Y int
}

func init() {
	ray.Init(driver, demo{})
}

func driver() {
	host, _ := os.Hostname()
	fmt.Printf("driver host: %s\n", host)

	{
		objRef := ray.RemoteCall("Hello", "ray", ray.WithTaskOption("num_cpus", 2))
		res, err := objRef.Get()
		assert(err, nil)
		fmt.Printf("call Hello -> %#v \n", res)
	}
	{
		objRef := ray.RemoteCall("MultiReturn", 42, "str")
		i, s, err := objRef.Get2()
		assert(err, nil)
		assert(i, 42)
		assert(s, "str")
	}
	{
		// high level api
		future := demoTasks.AddPoints([]Point{{1, 2}, {3, 4}}).Remote(ray.WithTaskOption("num_cpus", 2))
		res, err := future.Get()
		assert(err, nil)
		assert(res.X, 4)
		assert(res.Y, 6)
		fmt.Printf("call AddPoints -> %#v \n", res)
	}
	{
		f1 := demoTasks.Workload("driver task1").Remote()
		f2 := demoTasks.Workload("driver task2").Remote()
		t1, _ := f1.Get()
		t2, _ := f2.Get()
		fmt.Println("task results: ", t1, t2)
	}
	{
		res, err := ray.CallPythonCode("put('hello from python')")
		assert(err, nil)
		assert(res, "hello from python")

		res, err = ray.CallPythonCode("1/0")
		assert(err != nil, true)
	}
	{
		demoTasks.CallOtherTaskLowLevel().Remote()
		demoTasks.CallOtherTaskHighLevel().Remote()
	}

}

// raytasks
type demo struct{}

func (_ demo) Hello(name string) string {
	return fmt.Sprintf("Hello %s", name)
}

func (_ demo) Workload(name string) string {
	fmt.Println("Task", name, "started at", time.Now())
	time.Sleep(1 * time.Second)
	fmt.Println("Task", name, "finished at", time.Now())
	return fmt.Sprintf("Task %s success", name)
}

func (_ demo) Add(a, b int64) int64 {
	return a + b
}

func (_ demo) Nil(a, b int64) {
	return
}

func (_ demo) MultiReturn(i int, s string) (int, string) {
	return i, s
}

func (_ demo) AddPoints(points []Point) Point {
	res := Point{}
	for _, p := range points {
		res.X += p.X
		res.Y += p.Y
	}
	return res
}

func (_ demo) CallOtherTaskLowLevel() {
	objRef1 := ray.RemoteCall("Workload", "CallOtherTaskLowLevel task1")
	objRef2 := ray.RemoteCall("Workload", "CallOtherTaskLowLevel task2")

	t1, err1 := objRef1.Get()
	t2, err2 := objRef2.Get()
	assert(err1, nil)
	assert(err2, nil)
	fmt.Println("task results: ", t1, t2)
}

func (_ demo) CallOtherTaskHighLevel() {
	future1 := demoTasks.Workload("CallOtherTaskHighLevel task1").Remote()
	future2 := demoTasks.Workload("CallOtherTaskHighLevel task2").Remote()

	t1, err1 := future1.Get()
	t2, err2 := future2.Get()
	assert(err1, nil)
	assert(err2, nil)
	fmt.Println("task results: ", t1, t2)
}

func (_ demo) ErrorReturn() (string, error) {
	return "", fmt.Errorf("error")
}

// main 函数不会被调用，但不可省略
func main() {}
