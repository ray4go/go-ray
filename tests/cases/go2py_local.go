package cases

import (
	"encoding/binary"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/ray4go/go-ray/ray"
	"github.com/ray4go/go-ray/tests/tools"
	"github.com/stretchr/testify/require"
)

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

	AddTestCase("TestGoCallPy-local", func(assert *require.Assertions) {
		pid, err := ray.Get1[int](ray.LocalCallPyTask("get_pid"))
		assert.NoError(err)
		assert.Equal(os.Getpid(), pid)
	})

	AddTestCase("TestGoCallPy-local-threads", func(assert *require.Assertions) {
		var res int
		err := ray.LocalCallPyTask("busy", 1).GetInto(&res)
		assert.NoError(err)

		start := time.Now()
		threads := sync.Map{}
		var wg sync.WaitGroup
		wg.Add(10)
		for i := 0; i < 10; i++ {
			go func() {
				defer wg.Done()
				var threadId int
				err := ray.LocalCallPyTask("busy", 1).GetInto(&threadId)
				assert.NoError(err)
				//threads[threadId] = struct{}{}
				threads.Store(threadId, struct{}{})
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		count := 0
		threads.Range(func(key, value interface{}) bool {
			count++
			return true // 返回 true 继续遍历
		})
		assert.Equal(10, count, "should have 10 unique threads")
		assert.Less(elapsed, time.Duration(count)/2*time.Second, "should be faster than 2 seconds")
	})

	AddTestCase("TestGoCallPy-local-actor", func(assert *require.Assertions) {
		args := []any{1, "str", true, 3.0, []int{1, 2, 3}, map[string]int{"a": 1}}
		actor := ray.NewPyLocalInstance("PyActor", args...)
		res, err := actor.MethodCall("get_args").Get()
		assert.NoError(err)
		assert.True(tools.DeepEqualValues(args, res))

		res, err = actor.MethodCall("echo", args...).Get()
		assert.NoError(err)
		assert.True(tools.DeepEqualValues(args, res))

		obj := actor.MethodCall("hello", "world")
		res1, err := ray.Get1[string](obj)
		assert.NoError(err)
		assert.Equal("hello world", res1)
		err = obj.GetInto(&res1)
		assert.NoError(err)
		assert.Equal("hello world", res1)

		actor = ray.NewPyLocalInstance("PyActor")
		actor.MethodCall("no_return", "world")
		obj2 := actor.MethodCall("no_return", "world")
		res, err = obj2.Get()
		assert.NoError(err)
		err = ray.Get0(obj2)
		assert.NoError(err)
		err = obj2.GetInto()
		assert.NoError(err)

		obj3 := actor.MethodCall("single", 1)
		res3, err := ray.Get1[int](obj3)
		assert.NoError(err)
		assert.EqualValues(1, res3)
		err = obj3.GetInto(&res3)
		assert.NoError(err)
		assert.EqualValues(1, res3)

		obj4 := actor.MethodCall("no_args")
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
		obj5 := actor.MethodCall("single", t1)
		res5, err := ray.Get1[T1](obj5)
		assert.NoError(err)
		assert.Equal(t1, res5)
		err = obj5.GetInto(&res5)
		assert.NoError(err)
		assert.Equal(t1, res5)

		assert.NoError(actor.Close())
		assert.Error(actor.Close())
	})

	// Test LocalCallPyTask with complex types
	AddTestCase("TestGoCallPy-LocalCallComplex", func(assert *require.Assertions) {
		type ComplexData struct {
			ID       int               `json:"id"`
			Name     string            `json:"name"`
			Tags     []string          `json:"tags"`
			Metadata map[string]string `json:"metadata"`
		}

		input := ComplexData{
			ID:   42,
			Name: "test_item",
			Tags: []string{"tag1", "tag2", "tag3"},
			Metadata: map[string]string{
				"version": "1.0",
				"author":  "test",
			},
		}

		var result ComplexData
		err := ray.LocalCallPyTask("single", input).GetInto(&result)
		assert.NoError(err)
		assert.Equal(input, result)
	})

	// Test CallPythonCode error cases
	AddTestCase("TestGoCallPy-CallPythonCodeErrors", func(assert *require.Assertions) {
		// Test syntax error in Python code
		badCode := `
def bad_function(
    return "missing colon and closing paren"
`
		result := ray.CallPythonCode(badCode, 1)
		_, err := result.Get()
		assert.Error(err)
		assert.Contains(err.Error(), "Invalid python function code")

		// Test runtime error in Python code
		errorCode := `
def runtime_error():
    raise ValueError("Runtime error for testing")
`
		result2 := ray.CallPythonCode(errorCode)
		_, err2 := result2.Get()
		assert.Error(err2)
	})

	AddTestCase("TestGoCallPy-Uint64", func(assertions *require.Assertions) {
		random := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < 100; i++ {
			num := random.Uint64()
			obj := ray.RemoteCallPyTask("pack_uint64", num)
			data, err := ray.Get1[[]byte](obj)
			res := binary.LittleEndian.Uint64(data)
			assertions.NoError(err)
			assertions.Equal(num, res)

			obj2 := ray.LocalCallPyTask("pack_uint64", num)
			data, err = ray.Get1[[]byte](obj2)
			res = binary.LittleEndian.Uint64(data)
			assertions.NoError(err)
			assertions.Equal(num, res)
		}
	})

	// Test local Python call with various arguments
	AddTestCase("TestGoCallPy-local-args", func(assert *require.Assertions) {
		args, err := ray.LocalCallPyTask("echo", 1, "str", true, 3.14, []int{1, 2, 3}, map[string]int{"a": 1}).Get()
		assert.NoError(err)
		assert.True(tools.DeepEqualValues([]any{1, "str", true, 3.14, []int{1, 2, 3}, map[string]int{"a": 1}}, args))
	})

	// Test local Python call with GetInto
	AddTestCase("TestGoCallPy-local-GetInto", func(assert *require.Assertions) {
		type resType struct {
			A int
			B string
			C []uint8
		}
		obj := ray.LocalCallPyTask("single", resType{A: 1, B: "str", C: []byte("bytes")})
		var res1 resType
		err := obj.GetInto(&res1)
		assert.NoError(err)
		assert.Equal(resType{A: 1, B: "str", C: []byte("bytes")}, res1)

		obj2 := ray.LocalCallPyTask("single", map[any]any{"A": 1, "B": "str", "C": []byte("bytes")})
		var res2 resType
		err = obj2.GetInto(&res2)
		assert.NoError(err)
		assert.Equal(res1, res2)

		type resType2 struct {
			A *int
			B *string
			C *[]uint8
		}
		obj3 := ray.LocalCallPyTask("single", map[any]any{"A": 1, "B": "str", "C": []byte("bytes")})
		var res3 resType2
		err = obj3.GetInto(&res3)
		assert.NoError(err)
		assert.Equal(resType2{A: &res1.A, B: &res1.B, C: &res1.C}, res3)
	})

	// Test Python multiple return values - Go receives as single list (local call)
	AddTestCase("TestGoCallPy-local-MultipleReturns", func(assert *require.Assertions) {
		obj := ray.LocalCallPyTask("multiple_returns", 42, "hello")

		// Python returns multiple values, but Go receives as single list
		result, err := ray.Get1[[]any](obj)
		assert.NoError(err)
		assert.Len(result, 5)
		assert.Equal(int64(42), result[0])
		assert.Equal("hello", result[1])
		assert.Equal(int64(84), result[2])
		assert.Equal("HELLO", result[3])
		assert.Equal(true, result[4])
	})

	// Test Python functions that return different numbers of values (local call)
	AddTestCase("TestGoCallPy-local-VariousReturnCounts", func(assert *require.Assertions) {
		// Test three_returns - Python returns 3 values, Go gets as single list
		result3, err := ray.Get1[[]any](ray.LocalCallPyTask("three_returns", 10))
		assert.NoError(err)
		assert.Len(result3, 3)
		assert.Equal(int64(10), result3[0])
		assert.Equal(int64(20), result3[1])
		assert.Equal(int64(30), result3[2])

		// Test four_returns - Python returns 4 values, Go gets as single list
		result4, err := ray.Get1[[]any](ray.LocalCallPyTask("four_returns", 5))
		assert.NoError(err)
		assert.Len(result4, 4)
		assert.Equal(int64(5), result4[0])
		assert.Equal(int64(10), result4[1])
		assert.Equal(int64(15), result4[2])
		assert.Equal(int64(20), result4[3])

		// Test five_returns - Python returns 5 values, Go gets as single list
		result5, err := ray.Get1[[]any](ray.LocalCallPyTask("five_returns", 3))
		assert.NoError(err)
		assert.Len(result5, 5)
		assert.Equal(int64(3), result5[0])
		assert.Equal(int64(6), result5[1])
		assert.Equal(int64(9), result5[2])
		assert.Equal(int64(12), result5[3])
		assert.Equal(int64(15), result5[4])
	})

	// Test None value handling (local call)
	AddTestCase("TestGoCallPy-local-NoneValues", func(assert *require.Assertions) {
		obj := ray.LocalCallPyTask("return_none")

		// Test with pointer type (should get nil)
		var ptrInt *int
		err := obj.GetInto(&ptrInt)
		assert.NoError(err)
		assert.Nil(ptrInt)
	})

	// Test empty values (local call)
	AddTestCase("TestGoCallPy-local-EmptyValues", func(assert *require.Assertions) {
		vals, err := ray.Get1[[]any](ray.LocalCallPyTask("return_empty_values"))
		assert.NoError(err)
		assert.Len(vals, 5)
		assert.True(tools.DeepEqualValues([]interface{}{}, vals[0])) // empty list
		assert.Equal(map[any]interface{}{}, vals[1])                 // empty dict
		assert.Equal("", vals[2])                                    // empty string
		assert.Equal(int64(0), vals[3])                              // zero int
		assert.Equal(false, vals[4])                                 // false bool
	})

	// Test complex nested data structures (local call)
	AddTestCase("TestGoCallPy-local-ComplexTypes", func(assert *require.Assertions) {
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

		var result NestedStruct
		err := ray.LocalCallPyTask("complex_types", input).GetInto(&result)
		assert.NoError(err)
		assert.Equal(input, result)
	})

	// Test type conversion from Python to Go (local call)
	AddTestCase("TestGoCallPy-local-TypeConversion", func(assert *require.Assertions) {
		// Test GetInto with map
		var result map[string]interface{}
		err := ray.LocalCallPyTask("type_conversion_test").GetInto(&result)
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

	// Test large data transfer (local call)
	AddTestCase("TestGoCallPy-local-LargeData", func(assert *require.Assertions) {
		result, err := ray.Get1[[]int](ray.LocalCallPyTask("large_data_task"))
		assert.NoError(err)
		assert.Len(result, 10000)
		assert.Equal(0, result[0])
		assert.Equal(9999, result[9999])
	})

	// Test special float values (local call)
	AddTestCase("TestGoCallPy-local-SpecialFloats", func(assert *require.Assertions) {
		var result map[string]float64
		err := ray.LocalCallPyTask("return_special_floats").GetInto(&result)
		assert.NoError(err)

		assert.True(math.IsInf(result["inf"], 1), "should be positive infinity")
		assert.True(math.IsInf(result["neg_inf"], -1), "should be negative infinity")
		assert.True(math.IsNaN(result["nan"]), "should be NaN")
		assert.Equal(0.0, result["zero"])
		assert.Equal(0.0, result["neg_zero"]) // Go treats -0.0 same as 0.0
	})

	// Test large integers (local call)
	AddTestCase("TestGoCallPy-local-LargeIntegers", func(assert *require.Assertions) {
		result, err := ray.Get1[int64](ray.LocalCallPyTask("return_very_large_int"))
		assert.NoError(err)
		assert.Equal(int64(math.MaxInt64), result)
	})

	// Test map[string]struct{} equivalent (local call)
	AddTestCase("TestGoCallPy-local-MapStructValues", func(assert *require.Assertions) {
		// Send Go map[string]struct{} to Python
		goSet := map[string]struct{}{
			"item1": {},
			"item2": {},
			"item3": {},
		}
		var result1 map[string]interface{}
		err := ray.LocalCallPyTask("single", goSet).GetInto(&result1)
		assert.NoError(err)
		assert.Len(result1, 3)
		assert.Contains(result1, "item1")
		assert.Contains(result1, "item2")
		assert.Contains(result1, "item3")

		// Get Python equivalent back
		var result2 map[string]struct{}
		err = ray.LocalCallPyTask("map_with_struct_values").GetInto(&result2)
		assert.NoError(err)
		assert.Len(result2, 3)
		assert.Contains(result2, "key1")
		assert.Contains(result2, "key2")
		assert.Contains(result2, "key3")
	})

	// Test Unicode string handling (local call)
	AddTestCase("TestGoCallPy-local-UnicodeStrings", func(assert *require.Assertions) {
		var result map[string]string
		err := ray.LocalCallPyTask("unicode_strings").GetInto(&result)
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
		var result2 map[string]string
		err = ray.LocalCallPyTask("single", unicodeInput).GetInto(&result2)
		assert.NoError(err)
		assert.Equal(unicodeInput, result2)
	})

	// Test array operations with Python functions (local call)
	AddTestCase("TestGoCallPy-local-ArrayOperations", func(assert *require.Assertions) {
		inputArray := []int{1, 3, 6, 8, 10, 2}
		var result map[string]interface{}
		err := ray.LocalCallPyTask("array_operations", inputArray).GetInto(&result)
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

	// Test mixed type lists (local call)
	AddTestCase("TestGoCallPy-local-MixedTypeList", func(assert *require.Assertions) {
		result, err := ray.Get1[[]interface{}](ray.LocalCallPyTask("mixed_type_list"))
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

	// Test nested collections (local call)
	AddTestCase("TestGoCallPy-local-NestedCollections", func(assert *require.Assertions) {
		var result map[string]interface{}
		err := ray.LocalCallPyTask("nested_collections").GetInto(&result)
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

	// Test exception handling from Python (local call)
	AddTestCase("TestGoCallPy-local-ExceptionHandling", func(assert *require.Assertions) {
		_, err := ray.LocalCallPyTask("raise_exception").Get()
		assert.Error(err)
		assert.Contains(err.Error(), "Python exception for testing")
	})

	// Test data serialization edge cases (local call)
	AddTestCase("TestGoCallPy-local-SerializationEdgeCases", func(assert *require.Assertions) {
		// Test empty collections
		result, err := ray.Get1[[]interface{}](ray.LocalCallPyTask("return_empty_values"))
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

	// Test edge cases and boundary conditions (local call)
	AddTestCase("TestGoCallPy-local-EdgeCases", func(assert *require.Assertions) {
		result1, err := ray.Get1[int](ray.LocalCallPyTask("no_args"))
		assert.NoError(err)
		assert.Equal(42, result1)

		// Test nil/None arguments
		var result2 *string
		err = ray.LocalCallPyTask("single", nil).GetInto(&result2)
		assert.NoError(err)
		assert.Nil(result2)

		// Test very long string
		longString := string(make([]byte, 10000))
		for i := range longString {
			longString = longString[:i] + "a" + longString[i+1:]
		}
		result3, err := ray.Get1[string](ray.LocalCallPyTask("single", longString))
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
		var result4 map[string]interface{}
		err = ray.LocalCallPyTask("single", nested).GetInto(&result4)
		assert.NoError(err)
		assert.NotNil(result4["level1"])
	})

	// Test local actor advanced features
	AddTestCase("TestGoCallPy-local-ActorAdvanced", func(assert *require.Assertions) {
		actor := ray.NewPyLocalInstance("PyActor", "initial_state")

		// Test stateful operations
		result1, err := ray.Get1[string](actor.MethodCall("set_state", "counter", 10))
		assert.NoError(err)
		assert.Equal("set counter = 10", result1)

		result2, err := ray.Get1[int](actor.MethodCall("get_state", "counter"))
		assert.NoError(err)
		assert.Equal(10, result2)

		// Test multiple state operations
		actor.MethodCall("set_state", "name", "test")
		actor.MethodCall("set_state", "active", true)

		allState, err := ray.Get1[map[string]interface{}](actor.MethodCall("get_all_state"))
		assert.NoError(err)
		assert.Equal(int64(10), allState["counter"])
		assert.Equal("test", allState["name"])
		assert.Equal(true, allState["active"])

		// Test actor method with multiple returns - Go receives as single list
		result, err := ray.Get1[[]any](actor.MethodCall("multiply_returns", 3, "test"))
		assert.NoError(err)
		assert.Len(result, 4) // Python returns 4 values: a, b, a*2, b.upper()
		assert.Equal(int64(3), result[0])
		assert.Equal("test", result[1])
		assert.Equal(int64(6), result[2])
		assert.Equal("TEST", result[3])

		assert.NoError(actor.Close())
	})

	// Test local actor stateful operations
	AddTestCase("TestGoCallPy-local-ActorStateful", func(assert *require.Assertions) {
		actor := ray.NewPyLocalInstance("PyActor", "stateful_test")

		// Test counter functionality
		count1, err := ray.Get1[int](actor.MethodCall("increment_counter", 5))
		assert.NoError(err)
		assert.Equal(5, count1)

		count2, err := ray.Get1[int](actor.MethodCall("increment_counter", 3))
		assert.NoError(err)
		assert.Equal(8, count2)

		// Test default parameters (increment by 1)
		count3, err := ray.Get1[int](actor.MethodCall("increment_counter"))
		assert.NoError(err)
		assert.Equal(9, count3)

		// Reset counter
		resetCount, err := ray.Get1[int](actor.MethodCall("reset_counter"))
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

		results, err := ray.Get1[[]interface{}](actor.MethodCall("batch_operations", operations))
		assert.NoError(err)
		assert.Len(results, 7)
		assert.Equal("set name", results[0])
		assert.Equal("set age", results[1])
		assert.Equal("test_user", results[2])
		assert.Equal(int64(25), results[3])
		assert.Equal("not_found", results[4])
		assert.Equal("deleted age", results[5])
		assert.Equal("not_found", results[6])

		assert.NoError(actor.Close())
	})

	// Test CallPythonCode with advanced scenarios (local call)
	AddTestCase("TestGoCallPy-local-CallPythonCodeAdvanced", func(assert *require.Assertions) {
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
}
