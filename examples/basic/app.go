package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ray4go/go-ray/ray"
)

// raytasks
type tasks struct{}

func (tasks) Echo(args ...any) []any {
	fmt.Println("Echo", args)
	return args
}

func (tasks) Hello(name string) string {
	return fmt.Sprintf("Hello %s", name)
}

func (tasks) Busy(name string, duration time.Duration) string {
	fmt.Println("BusyTask", name, "started at", time.Now())
	time.Sleep(duration * time.Second)
	fmt.Println("BusyTask", name, "finished at", time.Now())
	return fmt.Sprintf("BusyTask %s success", name)
}

func (tasks) CallOtherTask(name string, duration time.Duration) string {
	obj := ray.RemoteCall("Busy", name, duration)
	res, err := ray.Get1[string](obj)
	fmt.Println("CallOtherTask: ", res, err)
	return res
}

// no return value
func (tasks) NoReturnVal() {
	return
}

// multiple return values
func (tasks) MultiReturn(i int, s string) (int, string) {
	return i, s
}

type Point struct {
	X, Y int
}

// custom struct can be used as parameter and return value
func (tasks) AddPoints(ps ...Point) Point {
	fmt.Printf("PointAddVar %#v\n", ps)
	res := Point{}
	for _, p := range ps {
		res.X += p.X
		res.Y += p.Y
	}
	return res
}

// rayactors
type actors struct{}

type counter struct {
	num int
}

func (actors) Counter(n int) *counter {
	fmt.Println("New counter actor, pid", os.Getpid())
	return &counter{num: n}
}

func (actor *counter) Incr(n int) int {
	fmt.Printf("Incr %d -> %d\n", actor.num, actor.num+n)
	actor.num += n
	return actor.num
}

func (actor *counter) Decr(n int) int {
	fmt.Printf("Decr %d -> %d\n", actor.num, actor.num-n)
	actor.num -= n
	return actor.num
}

func init() {
	ray.Init(tasks{}, actors{}, driver)
}

func driver() int {
	host, _ := os.Hostname()
	fmt.Printf("driver host: %s\n", host)

	{
		// ray task remote call
		obj1 := ray.RemoteCall("Echo", 1, "str", []byte("bytes"), []int{1, 2, 3})
		res1, err := obj1.GetAll()
		fmt.Printf("Echo return: %#v. Err: %v\n", res1, err)

		obj2 := ray.RemoteCall("Hello", "world")
		res2, err := ray.Get1[string](obj2)
		fmt.Printf("Hello return: %#v. Err: %v\n", res2, err)

		obj3 := ray.RemoteCall("NoReturnVal")
		err = ray.Get0(obj3)

		obj4 := ray.RemoteCall("MultiReturn", 1, "hello")
		res4_1, ret4_2, err := ray.Get2[int, string](obj4)
		fmt.Printf("MultiReturn return: %#v, %#v. Err: %v\n", res4_1, ret4_2, err)

		// pass objectRef as argument
		obj5 := ray.RemoteCall("Hello", "goRay")
		obj6 := ray.RemoteCall("Echo", obj5)
		res6, err := obj6.GetAll()
		fmt.Printf("Echo <- Hello return: %#v. Err: %v\n", res6, err)
	}
	{
		// Cancel task and ray.Wait
		obj1 := ray.RemoteCall("Busy", "Workload1", time.Duration(4), ray.Option("num_cpus", 1))
		obj2 := ray.RemoteCall("Busy", "Workload2", time.Duration(4))
		obj2.Cancel()
		_, err := obj2.GetAll()
		fmt.Println("Busy task is canceled: ", errors.Is(err, ray.ErrCancelled))

		ready, notReady, err := ray.Wait([]*ray.ObjectRef{obj1, obj2}, 1, ray.Option("timeout", 1.5))
		fmt.Printf("Ready:%#v, notReady:%#v, err:%v\n", ready, notReady, err)
	}
	{
		// ray.Put
		obj1, _ := ray.Put(Point{1, 2})
		obj3 := ray.RemoteCall("AddPoints", obj1, Point{3, 4})
		res, err := ray.Get1[Point](obj3)
		fmt.Println("AddPoints: ", res, err)
	}
	{
		// ray actor remote call
		cnt := ray.NewActor("Counter", 1)
		obj := cnt.RemoteCall("Incr", 1)
		res, err := ray.Get1[int](obj)
		fmt.Printf("Incr result: %#v. Err: %v\n", res, err)

		obj2 := cnt.RemoteCall("Decr", cnt.RemoteCall("Incr", 1))
		res2, err := ray.Get1[int](obj2)
		fmt.Printf("Decr result: %#v. Err: %v\n", res2, err)

		// get named actor
		ray.NewActor("Counter", 0, ray.Option("name", "cnt"))
		cnt2, err := ray.GetActor("cnt")
		fmt.Printf("GetActor cnt: %#v. Err: %v\n", cnt2, err)
		obj3 := cnt.RemoteCall("Incr", 1)
		res3, err := ray.Get1[int](obj3)
		fmt.Printf("Incr result: %#v. Err: %v\n", res3, err)

		// kill actor
		cnt2.Kill()
		obj4 := cnt2.RemoteCall("Incr", 1)
		_, err = ray.Get1[int](obj4)
		fmt.Println("Call actor after kill, got error:", err != nil)
	}
	{
		// goraygen wrappers
		fut1 := Hello("world").Remote()
		res1, err := fut1.Get()
		fmt.Printf("Hello return: %#v. Err: %v\n", res1, err)

		fut2 := NoReturnVal().Remote()
		err = fut2.Get()

		fut3 := MultiReturn(1, "hello").Remote()
		res3_1, res3_2, err := fut3.Get()
		fmt.Printf("MultiReturn return: %#v, %#v. Err: %v\n", res3_1, res3_2, err)

		// pass future as argument
		fut4 := Busy("task", time.Duration(1)).Remote()
		fut5 := Hello(fut4).Remote()
		res5, err := fut5.Get()
		fmt.Printf("Hello return: %#v. Err: %v\n", res5, err)

		// pass ray.Put as argument
		data, _ := ray.Put("shared data")
		fut6 := Hello(data).Remote()
		res5, err = fut6.Get()
		fmt.Printf("Hello return: %#v. Err: %v\n", res5, err)

		// cancel task
		fut7 := Busy("Workload1", time.Duration(4)).Remote()
		fut8 := Busy("Workload2", time.Duration(4)).Remote()
		fut7.ObjectRef().Cancel()
		_, err = fut7.Get()
		fmt.Println("Busy task is canceled: ", errors.Is(err, ray.ErrCancelled))

		// ray.Wait
		ready, notReady, err := ray.Wait([]*ray.ObjectRef{fut7.ObjectRef(), fut8.ObjectRef()}, 1, ray.Option("timeout", 1.5))
		fmt.Printf("Ready:%#v, notReady:%#v, err:%v\n", ready, notReady, err)

		// actor
		cnt := NewCounter(1).Remote()
		fut9 := Counter_Incr(cnt, 1).Remote()
		res9, err := fut9.Get()
		fmt.Printf("Counter Incr return: %#v. Err: %v\n", res9, err)

		// named actor
		NewCounter(1).Remote(ray.Option("name", "typed_cnt"))
		cnt2, err := ray.GetTypedActor[ActorCounter]("typed_cnt")
		fut10 := Counter_Incr(cnt, 1).Remote()
		res10, err := fut10.Get()
		fmt.Printf("Counter Incr return: %#v. Err: %v\n", res10, err)

		// kill actor
		cnt2.Kill()
		fut11 := Counter_Incr(cnt2, 1).Remote()
		_, err = fut11.Get()
		fmt.Println("Call actor after kill, got error:", err != nil)
	}
	return 0
}

// main function can't be omitted
func main() {}
