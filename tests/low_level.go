package main

import (
	"github.com/ray4go/go-ray/ray"
	"fmt"
	"github.com/stretchr/testify/require"
	"time"
)

func (_ testTask) Divide(a, b int64) (int64, int64) {
	return a / b, a % b
}

func init() {
	addTestCase("TestDivide", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("Divide", 16, 5, ray.WithTaskOption("num_cpus", 2))
		res, remainder, err := objRef.Get2()
		assert.Equal(int64(3), res)
		assert.Equal(int64(1), remainder)
		assert.Equal(nil, err)
	})

	addTestCase("TestDivide", func(assert *require.Assertions) {
		assert.Panics(func() {
			ray.RemoteCall("Divide", 5)
		})
	})
}

func (_ testTask) NoReturnVal(a, b int64) {
	return
}

func init() {
	addTestCase("TestNoReturnVal", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("NoReturnVal", 1, 2)
		err := objRef.Get0()
		assert.Nil(err)
	})
}

func (_ testTask) Busy(name string, duration time.Duration) string {
	time.Sleep(duration * time.Second)
	return fmt.Sprintf("BusyTask %s success", name)
}

func init() {
	addTestCase("TestCancel", func(assert *require.Assertions) {
		obj := ray.RemoteCall("Busy", "Workload1", 100)
		err := obj.Cancel()
		assert.Nil(err)

		_, err2 := obj.GetAll()
		assert.ErrorIs(err2, ray.ErrCancelled)

		ready, notReady, err := ray.Wait([]ray.ObjectRef{obj}, ray.NewOption("timeout", 0))
		assert.Equal(ready, []ray.ObjectRef{obj})
		assert.Empty(notReady)
	})

	addTestCase("TestTimeout", func(assert *require.Assertions) {
		obj := ray.RemoteCall("Busy", "Workload", 4)
		res, err := obj.GetAllTimeout(1)
		assert.Empty(res)
		assert.ErrorIs(err, ray.ErrTimeout)
	})
}

type Point struct {
	X, Y int
}

// 传递自定义类型的slice
func (_ testTask) AddPointSlice(ps []Point) Point {
	fmt.Printf("PointAddSlice %#v\n", ps)
	res := Point{}
	for _, p := range ps {
		res.X += p.X
		res.Y += p.Y
	}
	return res
}

// 2参数
func (_ testTask) Add2Points(p1, p2 Point) Point {
	fmt.Println("PointAdd2", p1, p2)
	return Point{
		X: p1.X + p2.X,
		Y: p1.Y + p2.Y,
	}
}

// 可变参数
func (_ testTask) AddPointsVar(ps ...Point) Point {
	fmt.Printf("PointAddVar %#v\n", ps)
	res := Point{}
	for _, p := range ps {
		res.X += p.X
		res.Y += p.Y
	}
	return res
}

func init() {
	addTestCase("TestPut", func(assert *require.Assertions) {
		obj1, e1 := ray.Put(Point{1, 2})
		obj2, e2 := ray.Put(Point{3, 4})
		assert.Nil(e1)
		assert.Nil(e2)

		obj3 := ray.RemoteCall("Add2Points", obj1, obj2)
		res, err := obj3.Get1()
		assert.Equal(Point{4, 6}, res)
		assert.Nil(err)
	})

	addTestCase("TestObjRefArg", func(assert *require.Assertions) {
		obj1 := ray.RemoteCall("AddPointSlice", []Point{{1, 2}, {3, 4}})
		obj2 := ray.RemoteCall("Add2Points", obj1, Point{5, 6})
		res, err := obj2.Get1()
		assert.Equal(Point{9, 12}, res)
		assert.Nil(err)
	})
}
