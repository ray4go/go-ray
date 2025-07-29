package cases

import (
	"fmt"
	"time"

	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

// Test coverage for APIs not covered by other test files

// Task for testing CallPythonCode
func (_ testTask) GetPythonEnvironment() string {
	result, err := ray.CallPythonCode(`
import sys
import os
result = f"Python {sys.version_info.major}.{sys.version_info.minor} on {sys.platform}"
write(result)
`)
	if err != nil {
		panic(fmt.Sprintf("CallPythonCode failed: %v", err))
	}
	return result
}

func (_ testTask) UsePythonLibrary() string {
	result, err := ray.CallPythonCode(`
import math
import json
data = {"pi": math.pi, "e": math.e}
write(json.dumps(data))
`)
	if err != nil {
		panic(fmt.Sprintf("Python library call failed: %v", err))
	}
	return result
}

// Tasks for testing advanced Wait scenarios
func (_ testTask) VariableDelayTask(taskId int, delayMs int) int {
	time.Sleep(time.Duration(delayMs) * time.Millisecond)
	return taskId * 100
}

func (_ testTask) RandomFailureTask(taskId int, shouldFail bool) int {
	if shouldFail {
		panic(fmt.Sprintf("Task %d failed intentionally", taskId))
	}
	time.Sleep(50 * time.Millisecond)
	return taskId * 10
}

// Task for testing ObjectRef edge cases
func (_ testTask) ReturnComplexObjectRef() map[string]interface{} {
	return map[string]interface{}{
		"id":      12345,
		"name":    "complex_object",
		"values":  []int{1, 2, 3, 4, 5},
		"nested":  map[string]string{"key1": "value1", "key2": "value2"},
		"success": true,
	}
}

func (_ testTask) ProcessComplexObjectRef(obj map[string]interface{}) string {
	id := int(obj["id"].(float64)) // JSON numbers come as float64
	name := obj["name"].(string)
	return fmt.Sprintf("Processed object %d: %s", id, name)
}

// Tasks for testing various option combinations
func (_ testTask) TaskWithAllOptions(value int) int {
	time.Sleep(100 * time.Millisecond)
	return value * 3
}

func (_ testTask) LongRunningTask(duration int, value string) string {
	time.Sleep(time.Duration(duration) * time.Second)
	return fmt.Sprintf("long_task_%s_completed", value)
}

// Tasks for testing edge cases with various data types
func (_ testTask) ProcessMixedTypes(
	intVal int,
	floatVal float64,
	stringVal string,
	boolVal bool,
	sliceVal []string,
	mapVal map[string]int,
) map[string]interface{} {
	return map[string]interface{}{
		"int_doubled":   intVal * 2,
		"float_squared": floatVal * floatVal,
		"string_upper":  fmt.Sprintf("PROCESSED_%s", stringVal),
		"bool_negated":  !boolVal,
		"slice_length":  len(sliceVal),
		"map_sum":       sumMapValues(mapVal),
	}
}

func sumMapValues(m map[string]int) int {
	sum := 0
	for _, v := range m {
		sum += v
	}
	return sum
}

// Actor for testing advanced actor options
type AdvancedOptionsActor struct {
	id        string
	startTime time.Time
	callCount int
}

func NewAdvancedOptionsActor(id string) *AdvancedOptionsActor {
	return &AdvancedOptionsActor{
		id:        id,
		startTime: time.Now(),
		callCount: 0,
	}
}

func (a *AdvancedOptionsActor) GetStatus() map[string]interface{} {
	a.callCount++
	return map[string]interface{}{
		"id":         a.id,
		"uptime":     time.Since(a.startTime).Seconds(),
		"call_count": a.callCount,
	}
}

func (a *AdvancedOptionsActor) ProcessWithDelay(delayMs int, data string) string {
	a.callCount++
	time.Sleep(time.Duration(delayMs) * time.Millisecond)
	return fmt.Sprintf("actor_%s_processed_%s", a.id, data)
}

func (a *AdvancedOptionsActor) MemoryIntensiveOperation(size int) int {
	a.callCount++
	// Allocate memory
	data := make([][]int, size)
	for i := range data {
		data[i] = make([]int, size)
		for j := range data[i] {
			data[i][j] = i + j
		}
	}

	// Compute something with the data
	sum := 0
	for _, row := range data {
		for _, val := range row {
			sum += val
		}
	}
	return sum
}

func init() {
	advancedActorName := RegisterActor(NewAdvancedOptionsActor)

	AddTestCase("TestCallPythonCode", func(assert *require.Assertions) {
		// Test basic Python code execution
		result, err := ray.CallPythonCode(`
x = 5
y = 10
result = x + y
write(str(result))
`)
		assert.Nil(err)
		assert.Equal("15", result)
	})

	AddTestCase("TestCallPythonCodeWithLibraries", func(assert *require.Assertions) {
		// Test Python code with standard libraries
		result, err := ray.CallPythonCode(`
import math
import json
data = {"sqrt_2": math.sqrt(2), "pi": math.pi}
write(json.dumps(data))
`)
		assert.Nil(err)
		assert.Contains(result, "sqrt_2")
		assert.Contains(result, "pi")
	})

	AddTestCase("TestCallPythonCodeMultipleWrites", func(assert *require.Assertions) {
		// Test multiple write calls
		result, err := ray.CallPythonCode(`
write("First line")
write("Second line")
write("Third line")
`)
		assert.Nil(err)
		assert.Contains(result, "First line")
		assert.Contains(result, "Second line")
		assert.Contains(result, "Third line")
	})

	AddTestCase("TestCallPythonCodeInTask", func(assert *require.Assertions) {
		// Test calling Python code from within a task
		ref := ray.RemoteCall("GetPythonEnvironment")
		result, err := ref.Get1()
		assert.Nil(err)
		assert.Contains(result.(string), "Python")
	})

	AddTestCase("TestAdvancedWaitScenarios", func(assert *require.Assertions) {
		// Test Wait with different timing scenarios
		refs := []ray.ObjectRef{
			ray.RemoteCall("VariableDelayTask", 1, 50),  // 50ms
			ray.RemoteCall("VariableDelayTask", 2, 100), // 100ms
			ray.RemoteCall("VariableDelayTask", 3, 200), // 200ms
			ray.RemoteCall("VariableDelayTask", 4, 300), // 300ms
		}

		// Wait for first 2 to complete
		ready, notReady, err := ray.Wait(refs, 2, ray.Option("timeout", 1.0))
		assert.Nil(err)
		assert.Len(ready, 2)
		assert.Len(notReady, 2)

		// Wait for all remaining
		allReady, allNotReady, err := ray.Wait(append(ready, notReady...), 4, ray.Option("timeout", 2.0))
		assert.Nil(err)
		assert.Len(allReady, 4)
		assert.Len(allNotReady, 0)
	})

	AddTestCase("TestWaitWithFailures", func(assert *require.Assertions) {
		// Test Wait behavior with failed tasks
		refs := []ray.ObjectRef{
			ray.RemoteCall("RandomFailureTask", 1, false), // Success
			ray.RemoteCall("RandomFailureTask", 2, true),  // Failure
			ray.RemoteCall("RandomFailureTask", 3, false), // Success
			ray.RemoteCall("RandomFailureTask", 4, true),  // Failure
		}

		// Wait should still work even with failures
		ready, notReady, err := ray.Wait(refs, 4, ray.Option("timeout", 2.0))
		assert.Nil(err)
		assert.Len(ready, 4)
		assert.Len(notReady, 0)

		// Check results (some should succeed, some should fail)
		successCount := 0
		failureCount := 0
		for _, ref := range ready {
			_, err := ref.Get1()
			if err != nil {
				failureCount++
			} else {
				successCount++
			}
		}
		assert.Equal(2, successCount)
		assert.Equal(2, failureCount)
	})

	// todo:
	//AddTestCase("TestComplexObjectRefChaining", func(assert *require.Assertions) {
	//	// Test chaining with complex objects
	//
	//	ref1 := ray.RemoteCall("ReturnComplexObjectRef")
	//	ref2 := ray.RemoteCall("ProcessComplexObjectRef", ref1)
	//
	//	result, err := ref2.Get1()
	//	assert.Nil(err)
	//	assert.Equal("Processed object 12345: complex_object", result)
	//})

	AddTestCase("TestMixedTypeProcessing", func(assert *require.Assertions) {
		// Test task with many different parameter types
		testMap := map[string]int{"a": 1, "b": 2, "c": 3}
		testSlice := []string{"item1", "item2", "item3"}

		ref := ray.RemoteCall("ProcessMixedTypes",
			42,        // int
			3.14,      // float64
			"test",    // string
			true,      // bool
			testSlice, // []string
			testMap,   // map[string]int
		)

		result, err := ref.Get1()
		assert.Nil(err)

		resultMap := result.(map[string]interface{})
		assert.Equal(int64(84), resultMap["int_doubled"])
		assert.InDelta(9.8596, resultMap["float_squared"], 0.0001)
		assert.Equal("PROCESSED_test", resultMap["string_upper"])
		assert.Equal(false, resultMap["bool_negated"])
		assert.Equal(3, resultMap["slice_length"])
		assert.Equal(6, resultMap["map_sum"]) // 1+2+3 = 6
	})

	AddTestCase("TestTaskWithAllOptionTypes", func(assert *require.Assertions) {
		// Test task with comprehensive options
		ref := ray.RemoteCall("TaskWithAllOptions", 10,
			ray.Option("num_cpus", 1),
			ray.Option("memory", 100*1024*1024), // 100MB
			ray.Option("max_retries", 2),
			ray.Option("retry_exceptions", true),
		)

		result, err := ref.Get1()
		assert.Nil(err)
		assert.Equal(30, result) // 10 * 3
	})

	AddTestCase("TestActorWithAdvancedOptions", func(assert *require.Assertions) {
		return // todo msgpack
		// Test actor creation with various options
		actor := ray.NewActor(advancedActorName, "advanced_test",
			ray.Option("num_cpus", 1),
			ray.Option("memory", 200*1024*1024), // 200MB
			ray.Option("max_restarts", 2),
		)

		// Test actor functionality
		ref := actor.RemoteCall("GetStatus")
		status, err := ref.Get1()
		assert.Nil(err)

		statusMap := status.(map[string]interface{})
		assert.Equal("advanced_test", statusMap["id"])
		assert.Equal(1, statusMap["call_count"])
		assert.Greater(statusMap["uptime"].(float64), 0.0)
	})

	AddTestCase("TestActorMemoryIntensiveWithOptions", func(assert *require.Assertions) {
		// Test memory-intensive actor operations
		actor := ray.NewActor(advancedActorName, "memory_test",
			ray.Option("num_cpus", 2),
			ray.Option("memory", 500*1024*1024), // 500MB
		)

		ref := actor.RemoteCall("MemoryIntensiveOperation", 50) // 50x50 matrix
		result, err := ref.Get1()
		assert.Nil(err)
		assert.Greater(result.(int), 0)
	})

	AddTestCase("TestObjectRefTimeoutHandling", func(assert *require.Assertions) {
		// Test various timeout scenarios
		ref := ray.RemoteCall("LongRunningTask", 2, "timeout_test") // 2 seconds

		// Short timeout should fail
		_, err1 := ref.GetAllTimeout(0.5) // 500ms
		assert.ErrorIs(err1, ray.ErrTimeout)

		// Longer timeout should succeed
		result, err2 := ref.GetAllTimeout(3.0) // 3 seconds
		assert.Nil(err2)
		assert.Equal([]interface{}{"long_task_timeout_test_completed"}, result)
	})

	AddTestCase("TestMultipleGetCallsOnSameObjectRef", func(assert *require.Assertions) {
		// Test that we can call Get methods multiple times on the same ObjectRef
		ref := ray.RemoteCall("ProcessMixedTypes", 5, 2.5, "multi", false, []string{"a"}, map[string]int{"x": 1})

		// First Get call
		result1, err1 := ref.Get1()
		assert.Nil(err1)

		// Second Get call on same ObjectRef
		result2, err2 := ref.Get1()
		assert.Nil(err2)

		// Results should be identical
		assert.Equal(result1, result2)
	})

	AddTestCase("TestGetActorNonExistent", func(assert *require.Assertions) {
		// Test getting non-existent actor
		_, err := ray.GetActor("non_existent_actor")
		assert.NotNil(err)
	})

	AddTestCase("TestObjectRefCancelAfterSuccess", func(assert *require.Assertions) {
		// Test canceling an already completed task
		ref := ray.RemoteCall("QuickTask", 100)

		// Wait for completion
		result, err := ref.Get1()
		assert.Nil(err)
		assert.Equal(200, result)

		// Cancel after completion (should be harmless)
		cancelErr := ref.Cancel()
		// This might succeed or fail depending on implementation
		// Both behaviors are valid for completed tasks
		_ = cancelErr
	})

	AddTestCase("TestLargeObjectRefBatch", func(assert *require.Assertions) {
		// Test handling many ObjectRefs at once
		var refs []ray.ObjectRef
		batchSize := 20

		for i := 0; i < batchSize; i++ {
			ref := ray.RemoteCall("QuickTask", i)
			refs = append(refs, ref)
		}

		// Wait for all to complete
		ready, notReady, err := ray.Wait(refs, batchSize, ray.Option("timeout", 5.0))
		assert.Nil(err)
		assert.Len(ready, batchSize)
		assert.Len(notReady, 0)

		// Verify all results
		for _, ref := range ready {
			result, err := ref.Get1()
			assert.Nil(err)
			// Results might not be in order, so we just check they're all valid
			assert.True(result.(int) >= 0 && result.(int) < batchSize*2)
		}
	})
}
