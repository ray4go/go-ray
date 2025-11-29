package main

import (
	"fmt"
	"os"

	"github.com/ray4go/go-ray/ray"
)

// raytasks
type Tasks struct{}

func (Tasks) Echo(args ...any) []any {
	fmt.Println("Echo", args)
	return args
}

func (Tasks) Hello(name string) string {
	return fmt.Sprintf("Hello %s", name)
}

// multiple return values
func (Tasks) MultiReturn(i int, s string) (int, string) {
	return i, s
}

type Point struct {
	X, Y int
}

// custom struct can be used as parameter and return value
func (Tasks) AddPoints(ps ...Point) Point {
	fmt.Printf("PointAddVar %#v\n", ps)
	res := Point{}
	for _, p := range ps {
		res.X += p.X
		res.Y += p.Y
	}
	return res
}

// rayactors
type Actors struct{}

type Counter struct {
	num int
}

func (Actors) Counter(n int) *Counter {
	fmt.Println("New actor, pid", os.Getpid())
	return &Counter{num: n}
}

func (actor *Counter) Incr(n int) int {
	fmt.Printf("Incr %d -> %d\n", actor.num, actor.num+n)
	actor.num += n
	return actor.num
}

func (actor *Counter) Decr(n int) int {
	fmt.Printf("Decr %d -> %d\n", actor.num, actor.num-n)
	actor.num -= n
	return actor.num
}

func init() {
	ray.Init(Tasks{}, Actors{}, driver)
}

func driver() int {
	// call python remote task
	{
		obj := ray.RemoteCallPyTask("echo", 1, "str", []byte("bytes"), []int{1, 2, 3})
		res, err := obj.GetAll()
		fmt.Println("go remote call python: echo", res, err)
	}
	{
		// pass objRef as parameter
		obj := ray.RemoteCallPyTask("echo", "test")
		obj2 := ray.RemoteCallPyTask("hello", obj)
		res, err := ray.Get1[string](obj2)
		fmt.Println("go remote call python: hello [obj as arg]", res, err)
	}
	{
		// cancel
		obj1 := ray.RemoteCallPyTask("busy", 4)
		obj2 := ray.RemoteCallPyTask("busy", 4)
		obj2.Cancel()
		_, err := obj2.GetAll()
		fmt.Println("Busy task is canceled: ", err)

		// wait
		ready, notReady, err := ray.Wait([]*ray.ObjectRef{obj1, obj2}, 1, ray.Option("timeout", 1.5))
		fmt.Printf("Ready:%#v, notReady:%#v, err:%v\n", ready, notReady, err)
	}
	{
		// put
		data, _ := ray.Put("shared data")
		obj := ray.RemoteCallPyTask("hello", data)
		res, err := ray.Get1[string](obj)
		fmt.Println("go remote call python: hello", res, err)
	}

	// call python remote actor
	{
		ref := ray.RemoteCall("Hello", "python")
		a := ray.NewPyActor("PyActor", ref, 12, []byte("bytes"), []int{1, 2, 3}, ray.Option("num_cpus", 1), ray.Option("name", "named"))
		fmt.Printf("PyActor %#v\n", a)

		obj0 := a.RemoteCall("hello", "hello from pyactor")
		res, err := obj0.GetAll()
		fmt.Printf("PyActor hello %#v %#v\n", res, err)

		obj1 := a.RemoteCall("get_args")
		res, err = obj1.GetAll()
		fmt.Printf("PyActor get_args %#v %#v\n", res, err)

		a2, err := ray.GetActor("named")
		fmt.Printf("GetActor %#v\n err%v\n", a2, err)

		obj3 := a2.RemoteCall("echo", []int{1, 2, 3})
		res, err = obj3.GetAll()
		fmt.Printf("PyActor echo %#v %#v\n", res, err)

		a2.Kill()
		obj4 := a2.RemoteCall("busy", 1)
		err = obj4.GetInto()
		fmt.Printf("PyActor busy, result: %v\n", err == nil)
	}

	// local call python task
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
		err := ray.LocalCallPyTask("busy", 1).GetInto()
		fmt.Println("go call python: busy", err == nil)
	}
	return 0
}

// main function can't be omitted
func main() {}
