package cases

import (
	"fmt"
	"time"

	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

func (testTask) Divide(a, b int64) (int64, int64) {
	return a / b, a % b
}

func init() {
	AddTestCase("TestDivide", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("Divide", 16, 5, ray.Option("num_cpus", 2))
		res, remainder, err := ray.Get2[int64, int64](objRef)
		assert.Equal(int64(3), res)
		assert.Equal(int64(1), remainder)
		assert.Equal(nil, err)
	})

	AddTestCase("TestDivide", func(assert *require.Assertions) {
		assert.Panics(func() {
			ray.RemoteCall("Divide", 5)
		})
	})

	AddTestCase("TestInvalidArg", func(assert *require.Assertions) {
		assert.Panics(func() {
			ray.RemoteCall("Divide", 16, ray.Option("num_cpus", 2), 5)
		}, "ray options must be the last arguments")

		assert.Panics(func() {
			ray.RemoteCall("Divide", 16, 3, 5)
		}, "arg num not match")
	})
}

func (testTask) NoReturnVal(a, b int64) {
	return
}

func init() {
	AddTestCase("TestNoReturnVal", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("NoReturnVal", 1, 2)
		err := ray.Get0(objRef)
		assert.NoError(err)
	})
}

func (testTask) Busy(name string, duration time.Duration) string {
	time.Sleep(duration * time.Second)
	return fmt.Sprintf("BusyTask %s success", name)
}

func init() {
	AddTestCase("TestCancel", func(assert *require.Assertions) {
		obj := ray.RemoteCall("Busy", "Workload1", 100)
		err := obj.Cancel()
		assert.NoError(err)

		_, err2 := obj.GetAll()
		assert.ErrorIs(err2, ray.ErrCancelled)

		ready, notReady, err := ray.Wait([]*ray.ObjectRef{obj}, 1, ray.Option("timeout", 0))
		assert.Equal(ready, []*ray.ObjectRef{obj})
		assert.Empty(notReady)
	})

	AddTestCase("TestTimeout", func(assert *require.Assertions) {
		obj := ray.RemoteCall("Busy", "Workload", 4)
		res, err := obj.GetAll(1)
		assert.Empty(res)
		assert.ErrorIs(err, ray.ErrTimeout)
	})
}

type Point struct {
	X, Y int
}

// 传递自定义类型的slice
func (testTask) AddPointSlice(ps []Point) Point {
	fmt.Printf("PointAddSlice %#v\n", ps)
	res := Point{}
	for _, p := range ps {
		res.X += p.X
		res.Y += p.Y
	}
	return res
}

// 2参数
func (testTask) Add2Points(p1, p2 Point) Point {
	fmt.Println("PointAdd2", p1, p2)
	return Point{
		X: p1.X + p2.X,
		Y: p1.Y + p2.Y,
	}
}

// 可变参数
func (testTask) AddPointsVar(ps ...Point) Point {
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
		res, err := ray.Get1[Point](obj3)
		assert.Equal(Point{4, 6}, res)
		assert.NoError(err)
	})

	AddTestCase("TestObjRefArg", func(assert *require.Assertions) {
		obj1 := ray.RemoteCall("AddPointSlice", []Point{{1, 2}, {3, 4}})
		obj2 := ray.RemoteCall("Add2Points", obj1, Point{5, 6})
		res, err := ray.Get1[Point](obj2)
		assert.Equal(Point{9, 12}, res)
		assert.NoError(err)
	})
}

// ray actor
type counter struct {
	num int
}

func (actorFactories) NewActor(n int) *counter {
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
	AddTestCase("TestActor", func(assert *require.Assertions) {
		actor := ray.NewActor("NewActor", 10)
		obj1 := actor.RemoteCall("Incr", 1)
		res1, err1 := ray.Get1[int](obj1)
		assert.Equal(nil, err1)
		assert.Equal(11, res1)

		obj2 := actor.RemoteCall("Decr", 2)    // 9
		obj3 := actor.RemoteCall("Incr", obj2) // 18
		res3, err3 := ray.Get1[int](obj3)
		assert.Equal(nil, err3)
		assert.Equal(18, res3)

		obj4 := actor.RemoteCall("Busy", 100)
		actor.Kill()
		err4 := ray.Get0(obj4)
		assert.NotNil(err4)
	})
}

func (testTask) MultipleReturns(arg1 any, arg2 any) (any, any) {
	return arg1, arg2
}

func (testTask) SingleReturns(arg any) any {
	return arg
}

func init() {
	AddTestCase("GetInto", func(assert *require.Assertions) {
		obj1 := ray.RemoteCall("MultipleReturns", 1, "hello")
		var (
			res1 int
			res2 string
		)
		err1 := obj1.GetInto(&res1, &res2)
		assert.Nil(err1)
		assert.EqualValues(1, res1)
		assert.Equal("hello", res2)

		type T1 struct {
			A int
			B string
			C []byte
			D *string
			E map[int]string
			F *T1
		}

		s := "str"
		t1 := T1{
			A: 1,
			B: "hello",
			C: []byte("world"),
			D: nil,
			E: map[int]string{
				1: "hello",
				2: "world",
			},
			F: &T1{
				A: 2,
				B: "hello",
				C: []byte("world"),
				D: &s,
			},
		}
		obj2 := ray.RemoteCall("MultipleReturns", t1, 1)
		obj2.DisableAutoRelease()
		_, err := obj2.GetAll()
		assert.NoError(err)

		var (
			res3 T1
			res4 any
		)
		err2 := obj2.GetInto(&res3, &res4)
		assert.Nil(err2)
		assert.Equal(t1, res3)
		assert.EqualValues(1, res4)
	})
}
