package cases

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
	AddTestCase("TestDivide", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("Divide", 16, 5, ray.Option("num_cpus", 2))
		res, remainder, err := objRef.Get2()
		assert.Equal(int64(3), res)
		assert.Equal(int64(1), remainder)
		assert.Equal(nil, err)
	})

	AddTestCase("TestDivide", func(assert *require.Assertions) {
		assert.Panics(func() {
			ray.RemoteCall("Divide", 5)
		})
	})
}

func (_ testTask) NoReturnVal(a, b int64) {
	return
}

func init() {
	AddTestCase("TestNoReturnVal", func(assert *require.Assertions) {
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
	AddTestCase("TestCancel", func(assert *require.Assertions) {
		obj := ray.RemoteCall("Busy", "Workload1", 100)
		err := obj.Cancel()
		assert.Nil(err)

		_, err2 := obj.GetAll()
		assert.ErrorIs(err2, ray.ErrCancelled)

		ready, notReady, err := ray.Wait([]ray.ObjectRef{obj}, 1, ray.Option("timeout", 0))
		assert.Equal(ready, []ray.ObjectRef{obj})
		assert.Empty(notReady)
	})

	AddTestCase("TestTimeout", func(assert *require.Assertions) {
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
	AddTestCase("TestPut", func(assert *require.Assertions) {
		obj1, e1 := ray.Put(Point{1, 2})
		obj2, e2 := ray.Put(Point{3, 4})
		assert.Nil(e1)
		assert.Nil(e2)

		obj3 := ray.RemoteCall("Add2Points", obj1, obj2)
		res, err := obj3.Get1()
		assert.Equal(Point{4, 6}, res)
		assert.Nil(err)
	})

	AddTestCase("TestObjRefArg", func(assert *require.Assertions) {
		obj1 := ray.RemoteCall("AddPointSlice", []Point{{1, 2}, {3, 4}})
		obj2 := ray.RemoteCall("Add2Points", obj1, Point{5, 6})
		res, err := obj2.Get1()
		assert.Equal(Point{9, 12}, res)
		assert.Nil(err)
	})
}

// ray actor
type counter struct {
	num int
}

func NewActor(n int) *counter {
	return &counter{num: n}
}

func (c *counter) Incr(n int) int {
	c.num += n
	return c.num
}

func (c *counter) Decr(n int) int {
	c.num -= n
	return c.num
}

func (c *counter) Busy(n time.Duration) {
	time.Sleep(n * time.Second)
}

func init() {
	name := RegisterActor(NewActor)
	AddTestCase("TestActor", func(assert *require.Assertions) {
		actor := ray.NewActor(name, 10)
		obj1 := actor.RemoteCall("Incr", 1)
		res1, err1 := obj1.Get1()
		assert.Equal(nil, err1)
		assert.Equal(11, res1)

		obj2 := actor.RemoteCall("Decr", 2)    // 9
		obj3 := actor.RemoteCall("Incr", obj2) // 18
		res3, err3 := obj3.Get1()
		assert.Equal(nil, err3)
		assert.Equal(18, res3)

		obj4 := actor.RemoteCall("Busy", 100)
		actor.Kill()
		err4 := obj4.Get0()
		assert.NotNil(err4)
	})
}
