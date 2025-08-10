package cases

import (
	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
	"os"
)

type cnt struct {
	num int
}

func NewCounter(n int) *cnt {
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

type GoNode struct {
	Val  int
	Next *GoNode
}

func (_ testTask) ReturnStruct() GoNode {
	return GoNode{Val: 1, Next: &GoNode{Val: 2, Next: &GoNode{Val: 3, Next: nil}}}
}

func init() {
	AddTestCase("TestPyCallGoEndpoint", func(assert *require.Assertions) {
		err := ray.LocalCallPyTask("start_python_tests").GetInto()
		assert.NoError(err)
	})
}
