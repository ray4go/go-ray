package cases

import (
	"fmt"
	"math"
	"time"

	"github.com/ray4go/go-ray/ray"
	"github.com/ray4go/go-ray/tests/tools"
	"github.com/stretchr/testify/require"
)

func (t TestTask) GenData(x int) int {
	return x
}

func (t TestTask) MultipleReturns2(a int, b string) (int, string, bool) {
	return a, b, true
}

func (t TestTask) ReturnSlice() []int {
	return []int{1, 2, 3, 4, 5}
}

func (t TestTask) ReturnMap() map[string]int {
	return map[string]int{"a": 1, "b": 2, "c": 3}
}

func init() {

	AddTestCase("TestGoCallPy-option", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("overwrite_ray_options", ray.Option("num_cpus", 1))
		err := ray.Get0(obj)
		assert.NoError(err)
	})

	AddTestCase("TestGoCallPy-args", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("echo", 1, "str", true, 3.14, []int{1, 2, 3}, map[string]int{"a": 1})
		args, err := ray.Get1[any](obj)
		assert.NoError(err)
		assert.True(tools.DeepEqualValues([]any{1, "str", true, 3.14, []int{1, 2, 3}, map[string]int{"a": 1}}, args))

		obj2 := ray.RemoteCallPyTask("echo", 1, 2, 3)
		obj2.DisableAutoRelease()
		args2, err := ray.Get1[[]any](obj2)
		assert.NoError(err)
		assert.True(tools.DeepEqualValues([]int{1, 2, 3}, args2))

		args3, err := ray.Get1[[]int64](obj2)
		assert.NoError(err)
		assert.True(tools.DeepEqualValues([]int{1, 2, 3}, args3))
	})

	AddTestCase("TestGoCallPy-args-obj", func(assert *require.Assertions) {
		obj := ray.RemoteCall("GenData", 44)
		obj2 := ray.RemoteCall("GenData", 12)
		obj3 := ray.RemoteCallPyTask("echo", 1, obj, obj, 2, obj2)
		args, err := ray.Get1[any](obj3)
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
		obj5.DisableAutoRelease()
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

	AddTestCase("TestGoCallPy-actor", func(assert *require.Assertions) {
		args := []any{1, "str", true, 3.0, []int{1, 2, 3}, map[string]int{"a": 1}}
		actor := ray.NewPyActor("PyActor", args...)
		res, err := actor.RemoteCall("get_args").GetAll()
		assert.NoError(err)
		assert.True(tools.DeepEqualValues(args, res[0]))

		res, err = actor.RemoteCall("echo", args...).GetAll()
		assert.NoError(err)
		assert.True(tools.DeepEqualValues(args, res[0]))

		obj := actor.RemoteCall("hello", "world")
		obj.DisableAutoRelease()
		res1, err := ray.Get1[string](obj)
		assert.NoError(err)
		assert.Equal("hello world", res1)
		err = obj.GetInto(&res1)
		assert.NoError(err)
		assert.Equal("hello world", res1)

		actor.RemoteCall("no_return", "world")
		obj2 := actor.RemoteCall("no_return", "world")
		obj2.DisableAutoRelease()
		res, err = obj2.GetAll()
		assert.NoError(err)
		err = ray.Get0(obj2)
		assert.NoError(err)
		err = obj2.GetInto()
		assert.NoError(err)

		obj3 := actor.RemoteCall("single", 1)
		obj3.DisableAutoRelease()
		res3, err := ray.Get1[int](obj3)
		assert.NoError(err)
		assert.EqualValues(1, res3)
		err = obj3.GetInto(&res3)
		assert.NoError(err)
		assert.EqualValues(1, res3)

		obj4 := actor.RemoteCall("no_args")
		res4, err := ray.Get1[int](obj4)
		assert.NoError(err)
		assert.EqualValues(42, res4)

		type T1 struct {
			Str   string
			Num   int
			Slice []int
			Map   map[string]int
			Point *T1
		}

		t1 := T1{
			Str:   "str",
			Num:   1,
			Slice: []int{1, 2, 3},
			Map:   map[string]int{"a": 1, "b": 2},
			Point: &T1{
				Str:   "str",
				Num:   1,
				Slice: []int{1, 2, 3},
				Map:   map[string]int{"a": 1, "b": 2},
			},
		}
		obj5 := actor.RemoteCall("single", t1)
		obj5.DisableAutoRelease()
		res5, err := ray.Get1[T1](obj5)
		assert.NoError(err)
		assert.Equal(t1, res5)
		err = obj5.GetInto(&res5)
		assert.NoError(err)
		assert.Equal(t1, res5)
	})

	AddTestCase("TestGoCallPy-getactor", func(assert *require.Assertions) {
		id := 123
		actor := ray.NewPyActor("PyActor", id, ray.Option("name", "test_go_actor"))
		actor2, err := ray.GetActor("test_go_actor")
		assert.NoError(err)

		res1, err1 := actor.RemoteCall("get_args").GetAll()
		res2, err2 := actor2.RemoteCall("get_args").GetAll()
		assert.NoError(err1)
		assert.NoError(err2)
		assert.True(tools.DeepEqualValues(res1, res2))

		err = actor.Kill()
		assert.NoError(err)

		err = actor2.RemoteCall("busy", 2).GetInto()
		assert.Error(err)

		_, err = ray.GetActor("test_go_actor")
		assert.Error(err)
	})

	// Test Python multiple return values - Go receives as single list
	AddTestCase("TestGoCallPy-MultipleReturns", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("multiple_returns", 42, "hello")
		obj.DisableAutoRelease()

		// Python returns multiple values, but Go receives as single list
		result, err := ray.Get1[[]any](obj)
		assert.NoError(err)
		assert.Len(result, 5)
		assert.Equal(int64(42), result[0])
		assert.Equal("hello", result[1])
		assert.Equal(int64(84), result[2])
		assert.Equal("HELLO", result[3])
		assert.Equal(true, result[4])

		// Test GetAll also returns single element containing the list
		allVals, err := obj.GetAll()
		assert.NoError(err)
		assert.Len(allVals, 1) // Single return value
		resultList := allVals[0].([]any)
		assert.Len(resultList, 5)
		assert.Equal(int64(42), resultList[0])
		assert.Equal("hello", resultList[1])
	})

	// Test Python functions that return different numbers of values
	AddTestCase("TestGoCallPy-VariousReturnCounts", func(assert *require.Assertions) {
		// Test three_returns - Python returns 3 values, Go gets as single list
		obj3 := ray.RemoteCallPyTask("three_returns", 10)
		result3, err := ray.Get1[[]any](obj3)
		assert.NoError(err)
		assert.Len(result3, 3)
		assert.Equal(int64(10), result3[0])
		assert.Equal(int64(20), result3[1])
		assert.Equal(int64(30), result3[2])

		// Test four_returns - Python returns 4 values, Go gets as single list
		obj4 := ray.RemoteCallPyTask("four_returns", 5)
		result4, err := ray.Get1[[]any](obj4)
		assert.NoError(err)
		assert.Len(result4, 4)
		assert.Equal(int64(5), result4[0])
		assert.Equal(int64(10), result4[1])
		assert.Equal(int64(15), result4[2])
		assert.Equal(int64(20), result4[3])

		// Test five_returns - Python returns 5 values, Go gets as single list
		obj5 := ray.RemoteCallPyTask("five_returns", 3)
		result5, err := ray.Get1[[]any](obj5)
		assert.NoError(err)
		assert.Len(result5, 5)
		assert.Equal(int64(3), result5[0])
		assert.Equal(int64(6), result5[1])
		assert.Equal(int64(9), result5[2])
		assert.Equal(int64(12), result5[3])
		assert.Equal(int64(15), result5[4])
	})

	// Test None value handling
	AddTestCase("TestGoCallPy-NoneValues", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("return_none")
		obj.DisableAutoRelease()

		// Test with pointer type (should get nil)
		var ptrInt *int
		err := obj.GetInto(&ptrInt)
		assert.NoError(err)
		assert.Nil(ptrInt)

		// Test with non-pointer type (should get zero value)
		var regularInt int
		err = obj.GetInto(&regularInt)
		assert.NoError(err)
		assert.Equal(0, regularInt)

		// Test GetAll
		vals, err := obj.GetAll()
		assert.NoError(err)
		assert.Len(vals, 1)
		assert.Nil(vals[0])
	})

	// Test empty values
	AddTestCase("TestGoCallPy-EmptyValues", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("return_empty_values")
		vals, err := ray.Get1[[]any](obj)
		assert.NoError(err)
		assert.Len(vals, 5)
		assert.True(tools.DeepEqualValues([]interface{}{}, vals[0])) // empty list
		assert.Equal(map[any]interface{}{}, vals[1])                 // empty dict
		assert.Equal("", vals[2])                                    // empty string
		assert.Equal(int64(0), vals[3])                              // zero int
		assert.Equal(false, vals[4])                                 // false bool
	})

	// Test complex nested data structures
	AddTestCase("TestGoCallPy-ComplexTypes", func(assert *require.Assertions) {
		type NestedStruct struct {
			Numbers []int             `msgpack:"numbers"`
			Mapping map[string]string `msgpack:"mapping"`
			Inner   *NestedStruct     `msgpack:"inner"`
		}

		input := NestedStruct{
			Numbers: []int{1, 2, 3},
			Mapping: map[string]string{"key1": "val1", "key2": "val2"},
			Inner: &NestedStruct{
				Numbers: []int{4, 5, 6},
				Mapping: map[string]string{"inner": "value"},
			},
		}

		obj := ray.RemoteCallPyTask("complex_types", input)
		var result NestedStruct
		err := obj.GetInto(&result)
		assert.NoError(err)
		assert.Equal(input, result)
	})

	// Test type conversion from Python to Go
	AddTestCase("TestGoCallPy-TypeConversion", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("type_conversion_test")

		// Test GetInto with map
		var result map[string]interface{}
		err := obj.GetInto(&result)
		assert.NoError(err)

		// Verify types are correctly converted
		assert.Equal(int64(42), result["int"])
		assert.Equal(3.14, result["float"])
		assert.Equal(true, result["bool"])
		assert.Equal("hello", result["str"])
		assert.Equal([]byte("world"), result["bytes"])
		assert.Equal([]interface{}{int64(1), int64(2), int64(3)}, result["list"])
		assert.True(tools.DeepEqualValues(map[string]interface{}{"a": int64(1), "b": int64(2)}, result["dict"]))
		assert.Nil(result["none"])

		// Test nested structures
		nested := result["nested"].(map[any]interface{})
		assert.NotNil(nested)
	})

	// Test Put() with Python tasks
	AddTestCase("TestGoCallPy-Put", func(assert *require.Assertions) {
		data := map[string]int{"x": 100, "y": 200}
		objRef, err := ray.Put(data)
		assert.NoError(err)

		// Use Put result as argument to Python task
		obj := ray.RemoteCallPyTask("single", objRef)
		var result map[string]int
		err = obj.GetInto(&result)
		assert.NoError(err)
		assert.Equal(data, result)
	})

	// Test Wait() with Python tasks
	AddTestCase("TestGoCallPy-Wait", func(assert *require.Assertions) {
		// Create multiple tasks with different completion times
		objs := []*ray.ObjectRef{
			ray.RemoteCallPyTask("timeout_task", 0.1),
			ray.RemoteCallPyTask("timeout_task", 0.2),
			ray.RemoteCallPyTask("timeout_task", 0.3),
		}

		// Wait for 2 out of 3 to complete
		ready, notReady, err := ray.Wait(objs, 2, ray.Option("timeout", 1.0))
		assert.NoError(err)
		assert.Len(ready, 2)
		assert.Len(notReady, 1)

		// Verify the ready ones are actually completed
		for _, readyObj := range ready {
			result, err := ray.Get1[string](readyObj)
			assert.NoError(err)
			assert.Equal("completed", result)
		}
	})

	// Test Cancel() with Python tasks
	AddTestCase("TestGoCallPy-Cancel", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("timeout_task", 5.0) // long running task

		// Cancel immediately
		err := obj.Cancel()
		assert.NoError(err)

		// Try to get result (should error)
		_, err = ray.Get1[string](obj, ray.WithTimeout(time.Millisecond*1000)) // 1 second timeout
		assert.Error(err)
	})

	// Test timeout functionality
	AddTestCase("TestGoCallPy-Timeout", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("timeout_task", 2.0) // 2 second task

		// Try to get with short timeout
		_, err := ray.Get1[string](obj, ray.WithTimeout(time.Millisecond*500)) // 0.5 second timeout
		assert.Error(err)
		assert.Contains(err.Error(), "timeout")
	})

	// Test large data transfer
	AddTestCase("TestGoCallPy-LargeData", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("large_data_task")
		result, err := ray.Get1[[]int](obj)
		assert.NoError(err)
		assert.Len(result, 10000)
		assert.Equal(0, result[0])
		assert.Equal(9999, result[9999])
	})

	// Test CallPythonCode with various scenarios
	AddTestCase("TestGoCallPy-CallPythonCode-Advanced", func(assert *require.Assertions) {
		// Test with imports
		codeWithImports := `
import math
import json
def calculate(radius):
    area = math.pi * radius * radius
    return {"radius": radius, "area": area}
`
		var result map[string]float64
		err := ray.CallPythonCode(codeWithImports, 5.0).GetInto(&result)
		assert.NoError(err)
		assert.Equal(5.0, result["radius"])
		assert.InDelta(math.Pi*25, result["area"], 0.001)

		// Test with complex return types
		listCode := `
def process_list(items):
    return [x * 2 for x in items if x % 2 == 0]
`
		var evenDoubled []int
		err = ray.CallPythonCode(listCode, []int{1, 2, 3, 4, 5, 6}).GetInto(&evenDoubled)
		assert.NoError(err)
		assert.Equal([]int{4, 8, 12}, evenDoubled)
	})

	// Test actor advanced features
	AddTestCase("TestGoCallPy-ActorAdvanced", func(assert *require.Assertions) {
		actor := ray.NewPyActor("PyActor", "initial_state")

		// Test stateful operations
		result1, err := ray.Get1[string](actor.RemoteCall("set_state", "counter", 10))
		assert.NoError(err)
		assert.Equal("set counter = 10", result1)

		result2, err := ray.Get1[int](actor.RemoteCall("get_state", "counter"))
		assert.NoError(err)
		assert.Equal(10, result2)

		// Test multiple state operations
		actor.RemoteCall("set_state", "name", "test")
		actor.RemoteCall("set_state", "active", true)

		allState, err := ray.Get1[map[string]interface{}](actor.RemoteCall("get_all_state"))
		assert.NoError(err)
		assert.Equal(int64(10), allState["counter"])
		assert.Equal("test", allState["name"])
		assert.Equal(true, allState["active"])

		// Test actor method with multiple returns - Go receives as single list
		result, err := ray.Get1[[]any](actor.RemoteCall("multiply_returns", 3, "test"))
		assert.NoError(err)
		assert.Len(result, 4) // Python returns 4 values: a, b, a*2, b.upper()
		assert.Equal(int64(3), result[0])
		assert.Equal("test", result[1])
		assert.Equal(int64(6), result[2])
		assert.Equal("TEST", result[3])
	})

	// Test Go to Python with ObjectRef chaining
	AddTestCase("TestGoCallPy-ObjectRefChaining", func(assert *require.Assertions) {
		// Create Go task results
		goObj1 := ray.RemoteCall("MultipleReturns2", 5, "chain")
		goObj2 := ray.RemoteCall("ReturnSlice")
		goObj3 := ray.RemoteCall("ReturnMap")

		// Pass Go results to Python tasks
		assert.Panics(func() {
			ray.RemoteCallPyTask("single", goObj1)
		})
		pyObj2 := ray.RemoteCallPyTask("single", goObj2)
		pyObj3 := ray.RemoteCallPyTask("single", goObj3)

		var sliceResult []int
		err := pyObj2.GetInto(&sliceResult)
		assert.NoError(err)
		assert.Equal([]int{1, 2, 3, 4, 5}, sliceResult)

		var mapResult map[string]int
		err = pyObj3.GetInto(&mapResult)
		assert.NoError(err)
		assert.Equal(map[string]int{"a": 1, "b": 2, "c": 3}, mapResult)
	})

	// Test actor with options
	AddTestCase("TestGoCallPy-ActorOptions", func(assert *require.Assertions) {
		// Create actor with specific options
		actor := ray.NewPyActor("PyActor", 123,
			ray.Option("name", "test_options_actor"),
			ray.Option("num_cpus", 0.5),
			ray.Option("max_restarts", 2))

		// Test that actor works
		result, err := ray.Get1[[]interface{}](actor.RemoteCall("get_args"))
		assert.NoError(err)
		assert.Equal(int64(123), result[0])

		// Test getting actor by name
		namedActor, err := ray.GetActor("test_options_actor")
		assert.NoError(err)

		result2, err := ray.Get1[[]interface{}](namedActor.RemoteCall("get_args"))
		assert.NoError(err)
		assert.Equal(result, result2)

		// Clean up
		err = actor.Kill()
		assert.NoError(err)
	})

	// Test special float values
	AddTestCase("TestGoCallPy-SpecialFloats", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("return_special_floats")
		var result map[string]float64
		err := obj.GetInto(&result)
		assert.NoError(err)

		assert.True(math.IsInf(result["inf"], 1), "should be positive infinity")
		assert.True(math.IsInf(result["neg_inf"], -1), "should be negative infinity")
		assert.True(math.IsNaN(result["nan"]), "should be NaN")
		assert.Equal(0.0, result["zero"])
		assert.Equal(0.0, result["neg_zero"]) // Go treats -0.0 same as 0.0
	})

	// Test large integers
	AddTestCase("TestGoCallPy-LargeIntegers", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("return_very_large_int")
		result, err := ray.Get1[int64](obj)
		assert.NoError(err)
		assert.Equal(int64(math.MaxInt64), result)
	})

	// Test map[string]struct{} equivalent
	AddTestCase("TestGoCallPy-MapStructValues", func(assert *require.Assertions) {
		// Send Go map[string]struct{} to Python
		goSet := map[string]struct{}{
			"item1": {},
			"item2": {},
			"item3": {},
		}
		obj1 := ray.RemoteCallPyTask("single", goSet)
		var result1 map[string]interface{}
		err := obj1.GetInto(&result1)
		assert.NoError(err)
		assert.Len(result1, 3)
		assert.Contains(result1, "item1")
		assert.Contains(result1, "item2")
		assert.Contains(result1, "item3")

		// Get Python equivalent back
		obj2 := ray.RemoteCallPyTask("map_with_struct_values")
		var result2 map[string]struct{}
		err = obj2.GetInto(&result2)
		assert.NoError(err)
		assert.Len(result2, 3)
		assert.Contains(result2, "key1")
		assert.Contains(result2, "key2")
		assert.Contains(result2, "key3")
	})

	// Test Unicode string handling
	AddTestCase("TestGoCallPy-UnicodeStrings", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("unicode_strings")
		var result map[string]string
		err := obj.GetInto(&result)
		assert.NoError(err)

		assert.Equal("你好世界", result["chinese"])
		assert.Equal("🚀🌟💻", result["emoji"])
		assert.Equal("tab:\t, newline:\n, quote:\"", result["special"])
		assert.Equal("", result["empty"])

		// Test sending Unicode from Go to Python
		unicodeInput := map[string]string{
			"japanese": "こんにちは",
			"arabic":   "مرحبا",
			"russian":  "привет",
		}
		obj2 := ray.RemoteCallPyTask("single", unicodeInput)
		var result2 map[string]string
		err = obj2.GetInto(&result2)
		assert.NoError(err)
		assert.Equal(unicodeInput, result2)
	})

	// Test error handling with malformed calls
	AddTestCase("TestGoCallPy-ErrorHandling", func(assert *require.Assertions) {
		// Test calling non-existent Python function
		defer func() {
			if r := recover(); r != nil {
				assert.Contains(fmt.Sprintf("%v", r), "not found")
			}
		}()
		ray.RemoteCallPyTask("non_existent_function")
	})

	// Test concurrent Python calls
	AddTestCase("TestGoCallPy-Concurrent", func(assert *require.Assertions) {
		const numTasks = 10
		var objs []*ray.ObjectRef

		// Start multiple Python tasks concurrently
		for i := 0; i < numTasks; i++ {
			obj := ray.RemoteCallPyTask("single", i)
			objs = append(objs, obj)
		}

		// Wait for all to complete
		ready, notReady, err := ray.Wait(objs, numTasks, ray.Option("timeout", 10.0))
		assert.NoError(err)
		assert.Len(ready, numTasks)
		assert.Len(notReady, 0)

		// Verify results
		for _, obj := range ready {
			result, err := ray.Get1[int](obj)
			assert.NoError(err)
			// Results might not be in order due to concurrent execution
			assert.GreaterOrEqual(result, 0)
			assert.Less(result, numTasks)
		}
	})

	// Test actor method with complex options
	AddTestCase("TestGoCallPy-ActorMethodOptions", func(assert *require.Assertions) {
		actor := ray.NewPyActor("PyActor", 100)

		// Test actor method call with options
		obj := actor.RemoteCall("busy", 1, ray.Option("num_returns", 1))
		result, err := ray.Get1[int](obj)
		assert.NoError(err)
		assert.Equal(1, result)
	})

	// Test nested ObjectRef usage
	AddTestCase("TestGoCallPy-NestedObjectRef", func(assert *require.Assertions) {
		// Create a Go task that returns data
		goObj := ray.RemoteCall("GenData", 42)

		// Use that as input to Python task
		pyObj1 := ray.RemoteCallPyTask("single", goObj)

		// Use Python result as input to another Python task
		pyObj2 := ray.RemoteCallPyTask("single", pyObj1)

		// Final result should be the original value
		finalResult, err := ray.Get1[int](pyObj2)
		assert.NoError(err)
		assert.Equal(42, finalResult)
	})

	// Test actor with stateful operations and batch processing
	AddTestCase("TestGoCallPy-ActorStateful", func(assert *require.Assertions) {
		actor := ray.NewPyActor("PyActor", "stateful_test")

		// Test counter functionality
		count1, err := ray.Get1[int](actor.RemoteCall("increment_counter", 5))
		assert.NoError(err)
		assert.Equal(5, count1)

		count2, err := ray.Get1[int](actor.RemoteCall("increment_counter", 3))
		assert.NoError(err)
		assert.Equal(8, count2)

		// Test default parameters (increment by 1)
		count3, err := ray.Get1[int](actor.RemoteCall("increment_counter"))
		assert.NoError(err)
		assert.Equal(9, count3)

		// Reset counter
		resetCount, err := ray.Get1[int](actor.RemoteCall("reset_counter"))
		assert.NoError(err)
		assert.Equal(0, resetCount)

		// Test batch operations
		operations := []map[string]interface{}{
			{"action": "set", "key": "name", "value": "test_user"},
			{"action": "set", "key": "age", "value": 25},
			{"action": "get", "key": "name"},
			{"action": "get", "key": "age"},
			{"action": "get", "key": "missing"},
			{"action": "delete", "key": "age"},
			{"action": "get", "key": "age"},
		}

		results, err := ray.Get1[[]interface{}](actor.RemoteCall("batch_operations", operations))
		assert.NoError(err)
		assert.Len(results, 7)
		assert.Equal("set name", results[0])
		assert.Equal("set age", results[1])
		assert.Equal("test_user", results[2])
		assert.Equal(int64(25), results[3])
		assert.Equal("not_found", results[4])
		assert.Equal("deleted age", results[5])
		assert.Equal("not_found", results[6])
	})

	// Test array operations with Python functions
	AddTestCase("TestGoCallPy-ArrayOperations", func(assert *require.Assertions) {
		inputArray := []int{1, 3, 6, 8, 10, 2}
		obj := ray.RemoteCallPyTask("array_operations", inputArray)

		var result map[string]interface{}
		err := obj.GetInto(&result)
		assert.NoError(err)

		// Check original array
		original := result["original"].([]interface{})
		assert.Len(original, 6)

		// Check length
		assert.Equal(int64(6), result["length"])

		// Check doubled values
		doubled := result["doubled"].([]interface{})
		assert.Len(doubled, 6)
		assert.Equal(int64(2), doubled[0])  // 1*2
		assert.Equal(int64(16), doubled[3]) // 8*2

		// Check filtered values (> 5)
		filtered := result["filtered"].([]interface{})
		assert.Len(filtered, 3) // 6, 8, 10

		// Check sum
		assert.Equal(int64(30), result["sum"]) // 1+3+6+8+10+2
	})

	// Test mixed type lists
	AddTestCase("TestGoCallPy-MixedTypeList", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("mixed_type_list")
		result, err := ray.Get1[[]interface{}](obj)
		assert.NoError(err)

		assert.Len(result, 7)
		assert.Equal(int64(1), result[0])
		assert.Equal("string", result[1])
		assert.Equal(3.14, result[2])
		assert.Equal(true, result[3])
		assert.Nil(result[4])                           // None
		assert.IsType([]interface{}{}, result[5])       // [1, 2]
		assert.IsType(map[any]interface{}{}, result[6]) // {"key": "value"}
	})

	// Test nested collections
	AddTestCase("TestGoCallPy-NestedCollections", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("nested_collections")
		var result map[string]interface{}
		err := obj.GetInto(&result)
		assert.NoError(err)

		// Check matrix (list of lists)
		matrix := result["matrix"].([]interface{})
		assert.Len(matrix, 3)
		firstRow := matrix[0].([]interface{})
		assert.Len(firstRow, 3)
		assert.Equal(int64(1), firstRow[0])

		// Check list of maps
		listOfMaps := result["list_of_maps"].([]interface{})
		assert.Len(listOfMaps, 3)
		firstMap := listOfMaps[0].(map[any]interface{})
		assert.Equal(int64(1), firstMap["a"])

		// Check map of lists
		mapOfLists := result["map_of_lists"].(map[any]interface{})
		numbers := mapOfLists["numbers"].([]interface{})
		assert.Len(numbers, 5)
		assert.Equal(int64(1), numbers[0])
	})

	// Test exception handling from Python
	AddTestCase("TestGoCallPy-ExceptionHandling", func(assert *require.Assertions) {
		obj := ray.RemoteCallPyTask("raise_exception")
		_, err := obj.GetAll()
		assert.Error(err)
		assert.Contains(err.Error(), "Python exception for testing")
	})

	// Test timeout with different scenarios
	AddTestCase("TestGoCallPy-TimeoutScenarios", func(assert *require.Assertions) {
		// Test successful completion within timeout
		obj1 := ray.RemoteCallPyTask("timeout_task", 0.1)
		result1, err := ray.Get1[string](obj1, ray.WithTimeout(time.Millisecond*2000)) // 2 second timeout
		assert.NoError(err)
		assert.Equal("completed", result1)

		// Test timeout on long task
		obj2 := ray.RemoteCallPyTask("timeout_task", 3.0)                      // 3 second task
		_, err = ray.Get1[string](obj2, ray.WithTimeout(time.Millisecond*500)) // 0.5 second timeout
		assert.Error(err)
	})

	// Test data serialization edge cases
	AddTestCase("TestGoCallPy-SerializationEdgeCases", func(assert *require.Assertions) {
		// Test empty collections
		obj := ray.RemoteCallPyTask("return_empty_values")
		result, err := ray.Get1[[]interface{}](obj)
		assert.NoError(err)
		assert.Len(result, 5)

		// Empty list
		assert.Equal([]interface{}{}, result[0])
		// Empty dict
		assert.Equal(map[any]interface{}{}, result[1])
		// Empty string
		assert.Equal("", result[2])
		// Zero
		assert.Equal(int64(0), result[3])
		// False
		assert.Equal(false, result[4])
	})

	// Test edge cases and boundary conditions
	AddTestCase("TestGoCallPy-EdgeCases", func(assert *require.Assertions) {
		obj1 := ray.RemoteCallPyTask("no_args")
		result1, err := ray.Get1[int](obj1)
		assert.NoError(err)
		assert.Equal(42, result1)

		// Test nil/None arguments
		obj2 := ray.RemoteCallPyTask("single", nil)
		var result2 *string
		err = obj2.GetInto(&result2)
		assert.NoError(err)
		assert.Nil(result2)

		// Test very long string
		longString := string(make([]byte, 10000))
		for i := range longString {
			longString = longString[:i] + "a" + longString[i+1:]
		}
		obj3 := ray.RemoteCallPyTask("single", longString)
		result3, err := ray.Get1[string](obj3)
		assert.NoError(err)
		assert.Len(result3, 10000)
		assert.Equal(longString, result3)

		// Test deeply nested structures
		nested := map[string]interface{}{
			"level1": map[string]interface{}{
				"level2": map[string]interface{}{
					"level3": map[string]interface{}{
						"level4": []interface{}{1, 2, 3, "deep"},
					},
				},
			},
		}
		obj4 := ray.RemoteCallPyTask("single", nested)
		var result4 map[string]interface{}
		err = obj4.GetInto(&result4)
		assert.NoError(err)
		assert.NotNil(result4["level1"])
	})

}
