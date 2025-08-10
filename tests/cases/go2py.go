package cases

import (
	"github.com/ray4go/go-ray/ray"
	"github.com/ray4go/go-ray/tests/tools"
	"github.com/stretchr/testify/require"
	"sync"
	"time"
)

func (t testTask) GenData(x int) int {
	return x
}

const pythonAddCode = `
def add(a, b):
    return a + b
`

func init() {
	AddTestCase("TestGoCallPy-CallPythonCode", func(assert *require.Assertions) {
		var res int
		err := ray.CallPythonCode(pythonAddCode, 1, 2).GetInto(&res)
		assert.NoError(err)
		assert.Equal(3, res)

		var res2 string
		err = ray.CallPythonCode(pythonAddCode, "ab", "cd").GetInto(&res2)
		assert.NoError(err)
		assert.Equal("abcd", res2)

		var res3 []int
		err = ray.CallPythonCode(pythonAddCode, []int{1, 2, 3}, []int{4, 5, 6}).GetInto(&res3)
		assert.NoError(err)
		assert.Equal([]int{1, 2, 3, 4, 5, 6}, res3)
	})

	AddTestCase("TestGoCallPy-option", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("overwrite_ray_options", ray.Option("num_cpus", 1))
		err := obj.Get0()
		assert.NoError(err)
	})

	AddTestCase("TestGoCallPy-args", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("echo", 1, "str", true, 3.14, []int{1, 2, 3}, map[string]int{"a": 1})
		args, err := obj.Get1()
		assert.NoError(err)
		assert.True(tools.DeepEqualValues([]any{1, "str", true, 3.14, []int{1, 2, 3}, map[string]int{"a": 1}}, args))
	})

	AddTestCase("TestGoCallPy-args-obj", func(assert *require.Assertions) {
		obj := ray.RemoteCall("GenData", 44)
		obj2 := ray.RemoteCall("GenData", 12)
		obj3 := ray.RemoteCallPyTask("echo", 1, obj, obj, 2, obj2)
		args, err := obj3.Get1()
		assert.NoError(err)
		assert.True(tools.DeepEqualValues([]any{1, 44, 44, 2, 12}, args))
	})

	AddTestCase("TestGoCallPy-GetInto", func(assert *require.Assertions) {
		type resType struct {
			A int
			B string
			C []uint8
		}
		obj4 := ray.RemoteCallPyTask("single", resType{A: 1, B: "str", C: []byte("bytes")})
		var res1 resType
		err := obj4.GetInto(&res1)
		assert.NoError(err)
		assert.Equal(resType{A: 1, B: "str", C: []byte("bytes")}, res1)

		obj5 := ray.RemoteCallPyTask("single", map[any]any{"A": 1, "B": "str", "C": []byte("bytes")})
		var res2 resType
		err = obj5.GetInto(&res2)
		assert.NoError(err)
		assert.Equal(res1, res2)

		type resType2 struct {
			A *int
			B *string
			C *[]uint8
		}
		var res3 resType2
		err = obj5.GetInto(&res3)
		assert.NoError(err)
		assert.Equal(resType2{A: &res1.A, B: &res1.B, C: &res1.C}, res3)
	})

	AddTestCase("TestGoCallPy-local-threads", func(assert *require.Assertions) {
		var res int
		err := ray.LocalCallPyTask("busy", 1).GetInto(&res)
		assert.NoError(err)

		start := time.Now()
		threads := map[any]struct{}{}
		var wg sync.WaitGroup
		wg.Add(10)
		for i := 0; i < 10; i++ {
			go func() {
				defer wg.Done()
				var threadId int
				err := ray.LocalCallPyTask("busy", 1).GetInto(&threadId)
				assert.NoError(err)
				threads[threadId] = struct{}{}
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		assert.Equal(10, len(threads), "should have 10 unique threads")
		assert.Less(elapsed, time.Duration(len(threads))/2*time.Second, "should be faster than 2 seconds")
	})
}
