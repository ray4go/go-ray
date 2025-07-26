package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ray4go/go-ray/ray"
)

type Point struct {
	X, Y int
}

type actor struct {
	num int
}

func NewActor(n int) *actor {
	return &actor{num: n}
}

func (actor *actor) Incr(n int) int {
	fmt.Printf("Incr %d -> %d\n", actor.num, actor.num+n)
	actor.num += n
	return actor.num
}

func (actor *actor) Decr(n int) int {
	fmt.Printf("Decr %d -> %d\n", actor.num, actor.num-n)
	actor.num -= n
	return actor.num
}

// raytasks
type demo struct{}

func (_ demo) BatchTask(batchId int, items []string) []string {
	result := make([]string, len(items))
	for i, item := range items {
		result[i] = item + "_batch_" + string(rune(batchId+'0'))
	}
	return result
}

func (_ demo) Hello(name string) string {
	return fmt.Sprintf("Hello %s", name)
}

func (_ demo) Busy(name string, duration time.Duration) string {
	fmt.Println("BusyTask", name, "started at", time.Now())
	time.Sleep(duration * time.Second)
	fmt.Println("BusyTask", name, "finished at", time.Now())
	return fmt.Sprintf("BusyTask %s success", name)
}

func (_ demo) Nest(name string, duration time.Duration) string {
	obj := ray.RemoteCall("Busy", name, duration)
	res, err := obj.Get1()
	fmt.Println("Nest: ", res, err)
	return res.(string)
}

// 传递自定义类型的slice
func (_ demo) AddPointSlice(ps []Point) Point {
	fmt.Printf("PointAddSlice %#v\n", ps)
	res := Point{}
	for _, p := range ps {
		res.X += p.X
		res.Y += p.Y
	}
	return res
}

// 2参数
func (_ demo) Add2Points(p1, p2 Point) Point {
	fmt.Println("PointAdd2", p1, p2)
	return Point{
		X: p1.X + p2.X,
		Y: p1.Y + p2.Y,
	}
}

// 可变参数
func (_ demo) AddPointsVar(ps ...Point) Point {
	fmt.Printf("PointAddVar %#v\n", ps)
	res := Point{}
	for _, p := range ps {
		res.X += p.X
		res.Y += p.Y
	}
	return res
}

// 无返回值
func (_ demo) NoReturnVal(a, b int64) {
	return
}

// 多返回值
func (_ demo) MultiReturn(i int, s string) (int, string) {
	return i, s
}

func init() {
	// raylog.Init(true)  // enable debug log
	ray.Init(driver, demo{}, map[string]any{"actor": NewActor})
}

const pycode = `
import threading
current_thread = threading.current_thread()
print(f"Thread name: {current_thread.name}")
`

func driver() {
	host, _ := os.Hostname()
	fmt.Printf("driver host: %s\n", host)
	{
		a := ray.NewActor("actor", 1)
		fmt.Printf("a: %#v\n", a)
		obj := a.RemoteCall("Incr", 1)
		fmt.Println("obj ", obj)
		res, err := obj.Get1()
		fmt.Println("res ", res, err)

		obj2 := a.RemoteCall("Incr", 1)
		obj3 := a.RemoteCall("Incr", obj2)
		fmt.Println("obj3 ", obj3)
		fmt.Println(obj3.Get1())
	}
	{
		obj := ray.RemoteCall("Nest", "nest", 2)
		res, err := obj.Get1()
		fmt.Println("res ", res, err)
	}
	{
		go func() {
			ray.CallPythonCode(pycode)
		}()
		time.Sleep(1 * time.Second) // wait for Python thread to start
	}
	{
		dataRef, _ := ray.Put([]string{"ref_item1", "ref_item2"})
		fmt.Println(dataRef)
		objRef := ray.RemoteCall("BatchTask", 99, dataRef,
			ray.Option("num_cpus", 1),
			ray.Option("memory", 50*1024*1024))
		fmt.Println(objRef.Get1())
	}
	{
		obj1 := ray.RemoteCall("Busy", "Workload1", 4, ray.Option("num_cpus", 1))
		obj2 := ray.RemoteCall("Busy", "Workload2", 4)
		obj2.Cancel()
		_, err := obj2.GetAll()
		fmt.Println("Workload2 is canceled: ", errors.Is(err, ray.ErrCancelled))
		ready, notReady, err := ray.Wait([]ray.ObjectRef{obj1, obj2}, ray.Option("timeout", 1.5))
		fmt.Printf("ready:%#v notReady:%#v err:%v\n", ready, notReady, err)
	}
	{
		// ray.Put
		obj1, _ := ray.Put(Point{1, 2})
		obj2, _ := ray.Put(Point{3, 4})
		obj3 := ray.RemoteCall("Add2Points", obj1, obj2)
		res, err := obj3.Get1()
		fmt.Println("Add2Points: ", res, err)
	}
	{
		// obj.GetAllTimeout()
		obj := ray.RemoteCall("Busy", "Workload", 4)
		fmt.Println(obj.GetAllTimeout(1))
	}
	{
		// MultiReturn
		obj := ray.RemoteCall("MultiReturn", 1, "hello")
		res1, res2, err := obj.Get2()
		fmt.Println(res1, res2, err)
	}
	{
		// pass objectRef as argument
		obj1 := ray.RemoteCall("AddPointSlice", []Point{{1, 2}, {3, 4}})
		obj2 := ray.RemoteCall("Add2Points", obj1, Point{5, 6})
		res, err := obj2.Get1()
		fmt.Println("Add2Points: ", res, err)
	}
}

// main 函数不会被调用，但不可省略
func main() {}
