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

type cnt struct {
	num int
}

// rayactors
type actors struct{}

// 匿名接收器
func (actors) Counter(n int) *cnt {
	fmt.Println("New actor, pid", os.Getpid())
	return &cnt{num: n}
}

func (actor *cnt) Incr(n int) int {
	fmt.Printf("Incr %d -> %d\n", actor.num, actor.num+n)
	actor.num += n
	return actor.num
}

func (actor *cnt) Decr(n int) int {
	fmt.Printf("Decr %d -> %d\n", actor.num, actor.num-n)
	actor.num -= n
	return actor.num
}

// raytasks
type demo struct{}

func (demo) BatchTask(batchId int, items []string) []string {
	result := make([]string, len(items))
	for i, item := range items {
		result[i] = item + "_batch_" + string(rune(batchId+'0'))
	}
	return result
}

func (demo) Echo(args ...any) []any {
	fmt.Println("Echo", args)
	return args
}

func (demo) Hello(name string) string {
	return fmt.Sprintf("Hello %s", name)
}

func (demo) Busy(name string, duration time.Duration) string {
	fmt.Println("BusyTask", name, "started at", time.Now())
	time.Sleep(duration * time.Second)
	fmt.Println("BusyTask", name, "finished at", time.Now())
	return fmt.Sprintf("BusyTask %s success", name)
}

func (demo) Nest(name string, duration time.Duration) string {
	obj := ray.RemoteCall("Busy", name, duration)
	res, err := ray.Get1[string](obj)
	fmt.Println("Nest: ", res, err)
	return res
}

// 传递自定义类型的slice
func (demo) AddPointSlice(ps []Point) Point {
	fmt.Printf("PointAddSlice %#v\n", ps)
	res := Point{}
	for _, p := range ps {
		res.X += p.X
		res.Y += p.Y
	}
	return res
}

// 2参数
func (demo) Add2Points(p1, p2 Point) Point {
	fmt.Println("PointAdd2", p1, p2)
	return Point{
		X: p1.X + p2.X,
		Y: p1.Y + p2.Y,
	}
}

// 可变参数
func (demo) AddPointsVar(ps ...Point) Point {
	fmt.Printf("PointAddVar %#v\n", ps)
	res := Point{}
	for _, p := range ps {
		res.X += p.X
		res.Y += p.Y
	}
	return res
}

// 无返回值
func (demo) NoReturnVal(a, b int64) {
	return
}

// 多返回值
func (demo) MultiReturn(i int, s string) (int, string) {
	return i, s
}

func (demo) CallPython() {
	{
		var res []any
		err := ray.LocalCallPyTask("echo", 1, "str", []byte("bytes"), []int{1, 2, 3}).GetInto(&res)
		fmt.Println("go call python: echo", res, err)
	}
	{
		var res string
		err := ray.LocalCallPyTask("hello", "from go").GetInto(&res)
		fmt.Println("go call python: hello", res, err)
	}
	{
		err := ray.LocalCallPyTask("no_return", "").GetInto()
		fmt.Println("go call python: no_return", err)
	}
}

func (demo) RemoteCallPython() {
	{
		obj := ray.RemoteCallPyTask("echo", 1, "str", []byte("bytes"), []int{1, 2, 3})
		res, err := obj.GetAll()
		fmt.Println("go remote call python: echo", res, err)
	}
	{
		obj := ray.RemoteCallPyTask("hello", "from remote go")
		res, err := obj.GetAll()
		fmt.Println("go remote call python: hello", res, err)

		obj2 := ray.RemoteCallPyTask("echo", 1, obj)
		res, err = obj2.GetAll()
		fmt.Println("go remote call python: echo [obj as arg]", res, err)
	}
	{
		obj := ray.RemoteCallPyTask("no_return", "")
		_, err := obj.GetAll()
		fmt.Println("go remote call python: no_return", err)
	}

	{
		ref := ray.RemoteCall("Hello", "hello from ref")
		a := ray.NewPyActor("PyActor", ref, 12, []byte("bytes"), []int{1, 2, 3}, ray.Option("num_cpus", 1), ray.Option("name", "named"))
		fmt.Printf("PyActor %#v\n", a)

		obj0 := a.RemoteCall("hello", "hello from pyactor")
		res, err := obj0.GetAll()
		fmt.Printf("PyActor hello %#v %#v\n", res, err)

		obj1 := a.RemoteCall("get_args")
		res, err = obj1.GetAll()
		fmt.Printf("PyActor get_args %#v %#v\n", res, err)
		obj2 := a.RemoteCall("echo", 2.13, ref, "str", []byte("bytes"), []int{1, 2, 3})
		res, err = obj2.GetAll()
		fmt.Printf("PyActor echo %#v %#v\n", res, err)

		a2, err := ray.GetActor("named")
		fmt.Printf("GetActor %#v\n err%v\n", a2, err)

		obj3 := a2.RemoteCall("echo", []int{1, 2, 3})
		res, err = obj3.GetAll()
		fmt.Printf("PyActor echo %#v %#v\n", res, err)

		a2.Kill()
		obj4 := a2.RemoteCall("busy", 1)
		res, err = obj4.GetAll()
		fmt.Printf("PyActor busy %#v %#v\n", res, err != nil)
	}

}

func init() {
	ray.Init(demo{}, actors{}, driver)
}

func driver() int {
	host, _ := os.Hostname()
	fmt.Printf("driver host: %s\n", host)
	{
		a := ray.NewActor("Counter", 1)
		fmt.Printf("a: %#v\n", a)
		obj := a.RemoteCall("Incr", 1)
		fmt.Println("obj ", obj)
		res, err := ray.Get1[int](obj)
		fmt.Println("res ", res, err)

		obj2 := a.RemoteCall("Incr", 1)
		obj3 := a.RemoteCall("Incr", obj2)
		fmt.Println("obj3 ", obj3)
		fmt.Println(obj3.GetAll())
	}
	{
		obj := ray.RemoteCall("Nest", "nest", 2)
		res, err := obj.GetAll()
		fmt.Println("res ", res, err)
	}
	{
		obj1 := ray.RemoteCall("Busy", "Workload1", 4, ray.Option("num_cpus", 1))
		obj2 := ray.RemoteCall("Busy", "Workload2", 4)
		obj2.Cancel()
		_, err := obj2.GetAll()
		fmt.Println("Workload2 is canceled: ", errors.Is(err, ray.ErrCancelled))
		ready, notReady, err := ray.Wait([]ray.ObjectRef{obj1, obj2}, 1, ray.Option("timeout", 1.5))
		fmt.Printf("ready:%#v notReady:%#v err:%v\n", ready, notReady, err)
	}
	{
		// ray.Put
		obj1, _ := ray.Put(Point{1, 2})
		obj2, _ := ray.Put(Point{3, 4})
		obj3 := ray.RemoteCall("Add2Points", obj1, obj2)
		res, err := obj3.GetAll()
		fmt.Println("Add2Points: ", res, err)
	}
	{
		// obj.GetAll()
		obj := ray.RemoteCall("Busy", "Workload", 4)
		fmt.Println(obj.GetAll(1))
	}
	{
		// MultiReturn
		obj := ray.RemoteCall("MultiReturn", 1, "hello")
		res1, res2, err := ray.Get2[int, string](obj)
		fmt.Println(res1, res2, err)
	}
	{
		// pass objectRef as argument
		obj1 := ray.RemoteCall("AddPointSlice", []Point{{1, 2}, {3, 4}})
		obj2 := ray.RemoteCall("Add2Points", obj1, Point{5, 6})
		res, err := obj2.GetAll()
		fmt.Println("Add2Points: ", res, err)
	}
	return 0
}

// main 函数不会被调用，但不可省略
func main() {}
