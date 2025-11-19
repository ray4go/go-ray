package cases

import (
	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

// Test Cross-Language Task/Actor Creation and Invocation
// Go actor used: py2go.go:GoNewCounter
// Python actor used: go2py.py:PyActor

func init() {
	AddTestCase("TestCrosslang:pass go objref to py task", func(assert *require.Assertions) {
		ref := ray.RemoteCall("Hello", "wang")
		ref2 := ray.RemoteCall("Single", 123)
		ref3 := ray.RemoteCallPyTask("echo", ref, ref2)
		results, err := ray.Get1[[]any](ref3)

		assert.NoError(err)
		assert.Equal(2, len(results))
		assert.Equal("hello wang", results[0])
		assert.Equal(int64(123), results[1])
	})

	// for pass py objref to go task, see
	// py2go.py:test_pass_py_objref_to_go

	AddTestCase("TestCrosslang:create py actor in go", func(assert *require.Assertions) {
		actor := ray.NewPyActor("PyActor", 10, "str")
		assert.NotNil(actor)

		ref1 := actor.RemoteCall("get_args")
		args, err := ray.Get1[[]any](ref1)
		assert.NoError(err)
		assert.Equal(2, len(args))
		assert.Equal(int64(10), args[0])
		assert.Equal("str", args[1])

		ref2 := actor.RemoteCall("echo", ray.RemoteCall("Single", 123))
		results, err := ray.Get1[[]int](ref2)
		assert.NoError(err)
		assert.Equal(1, len(results))
		assert.Equal(123, results[0])

	})

	AddTestCase("TestCrosslang:create go actor in py", func(assert *require.Assertions) {
		res := ray.CallPythonCode(`
import goray, time
def create_go_actor():
	global actor
	actorcls = goray.golang_actor_class("GoNewCounter", num_cpus=0)
	actor = actorcls.options(name="TestCrosslangGoCounterInPy").remote(20)
	return True
		`)
		v, err := ray.Get1[bool](res)
		assert.NoError(err)
		assert.True(v)

		actor, err2 := ray.GetActor("TestCrosslangGoCounterInPy")
		assert.NoError(err2)
		assert.NotNil(actor)

		ref1 := actor.RemoteCall("Incr", 15)
		result1, err3 := ray.Get1[int](ref1)
		assert.NoError(err3)
		assert.Equal(35, result1)

	})

	AddTestCase("TestCrosslang:get go actor in py", func(assert *require.Assertions) {
		actor := ray.NewActor("GoNewCounter", 4, ray.Option("name", "TestCrosslangGoCounter"), ray.Option("num_cpus", 0.01))
		assert.NotNil(actor)

		ref1 := actor.RemoteCall("Incr", 5)
		result1, err := ray.Get1[int](ref1)
		assert.NoError(err)
		assert.Equal(9, result1)

		var res bool
		ray.CallPythonCode(`
import ray
def get_go_actor_in_py():
	actor = ray.get_actor("TestCrosslangGoCounter")
	res1 = actor.Echo.remote(1, "2", 3.0)
	assert ray.get(res1) == [1, "2", 3.0]

	res2 = actor.Incr.remote(10)
	assert ray.get(res2) == 19

	res3 = actor.Single.remote(ray.put({"key": "value"}))
	assert ray.get(res3) == {"key": "value"}
	return True
`).GetInto(&res)
		assert.True(res)
	})

	AddTestCase("TestCrosslang:get py actor in py", func(assert *require.Assertions) {
		actor := ray.NewPyActor("PyActor", ray.Option("name", "TestCrosslangPyActor"))
		assert.NotNil(actor)

		actor.RemoteCall("set_state", "k", 5)

		var res bool
		err := ray.CallPythonCode(`
import ray
def get_py_actor_in_py():
	actor = ray.get_actor("TestCrosslangPyActor")
	state = actor.get_state.remote("k")
	assert ray.get(state) == 5

	echo_res = actor.echo.remote("a", 2, 3.0)
	assert ray.get(echo_res) == ("a", 2, 3.0)

	single_res = actor.single.remote([1, 2, 3])
	assert ray.get(single_res) == [1, 2, 3]

	return True
`).GetInto(&res)
		assert.NoError(err)
		assert.True(res)

	})

}
