package cases

import (
	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
	"os"
	"time"
)

type cnt struct {
	num int
}

func (actorFactories) GoNewCounter(n int) *cnt {
	return &cnt{num: n}
}

func (actor *cnt) Incr(n int) int {
	actor.num += n
	return actor.num
}

func (actor *cnt) Decr(n int) int {
	actor.num -= n
	return actor.num
}

func (actor *cnt) Pid() int {
	return os.Getpid()
}

func (_ *cnt) Echo(args ...any) []any {
	return args
}

func (_ *cnt) Single(arg any) any {
	return arg
}

func (_ *cnt) Hello(name string) string {
	return "hello " + name
}

func (_ *cnt) NoReturn(name string) {
}

func (_ *cnt) BusySleep(second int) {
	time.Sleep(time.Duration(second) * time.Second)
}

type GoNode struct {
	Val  int
	Next *GoNode
}

func (testTask) ReturnStruct() GoNode {
	return GoNode{Val: 1, Next: &GoNode{Val: 2, Next: &GoNode{Val: 3, Next: nil}}}
}

func (testTask) Pid() int {
	return os.Getpid()
}

func (testTask) Echo(args ...any) []any {
	return args
}

func (testTask) Single(arg any) any {
	return arg
}

func (testTask) Hello(name string) string {
	return "hello " + name
}

func (testTask) NoReturn(name string) {
}

func (testTask) BusySleep(second int) {
	time.Sleep(time.Duration(second) * time.Second)
}

func init() {
	AddTestCase("TestPyCallGo", func(assert *require.Assertions) {
		var ret int
		err := ray.LocalCallPyTask("start_python_tests").GetInto(&ret)
		assert.NoError(err)
		assert.Equal(0, ret)
	})
}
