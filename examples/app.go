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

func init() {
	// raylog.Init(true)  // enable debug log
	ray.Init(driver, demo{})
}

func driver() {
	host, _ := os.Hostname()
	fmt.Printf("driver host: %s\n", host)
	{
		obj1 := ray.RemoteCall("Busy", "Workload1", 4, ray.WithTaskOption("num_cpus", 1))
		obj2 := ray.RemoteCall("Busy", "Workload2", 4)
		obj2.Cancel()
		_, err := obj2.GetAll()
		fmt.Println("Workload2 is canceled: ", errors.Is(err, ray.ErrCancelled))
		ready, notReady, err := ray.Wait([]ray.ObjectRef{obj1, obj2}, ray.NewOption("timeout", 1.5))
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

// raytasks
type demo struct{}

func (_ demo) Hello(name string) string {
	return fmt.Sprintf("Hello %s", name)
}

func (_ demo) Busy(name string, duration time.Duration) string {
	fmt.Println("BusyTask", name, "started at", time.Now())
	time.Sleep(duration * time.Second)
	fmt.Println("BusyTask", name, "finished at", time.Now())
	return fmt.Sprintf("BusyTask %s success", name)
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

// main 函数不会被调用，但不可省略
func main() {}
