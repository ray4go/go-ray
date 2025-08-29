package cases

import (
	"github.com/ray4go/go-ray/tests/tools"
	"fmt"
	"time"

	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

// Test edge cases, error recovery, and robustness scenarios

// Tasks for testing boundary conditions
func (_ testTask) EmptySliceTask(data []int) int {
	return len(data)
}

func (_ testTask) NilMapTask(data map[string]int) int {
	if data == nil {
		return -1
	}
	return len(data)
}

func (_ testTask) ZeroValueTask(s string, i int, f float64, b bool) map[string]interface{} {
	return map[string]interface{}{
		"string_empty": s == "",
		"int_zero":     i == 0,
		"float_zero":   f == 0.0,
		"bool_false":   b == false,
	}
}

func (_ testTask) LargeStringTask(size int) string {
	// Create a large string
	result := make([]byte, size)
	for i := range result {
		result[i] = byte('A' + (i % 26))
	}
	return string(result)
}

func (_ testTask) DeepNestedStructTask() map[string]interface{} {
	return map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{
				"level3": map[string]interface{}{
					"level4": map[string]interface{}{
						"data": []int{1, 2, 3, 4, 5},
						"info": "deeply nested",
					},
				},
			},
		},
	}
}

// Tasks that test numeric edge cases
func (_ testTask) NumericBoundaryTask(maxInt int, minInt int, largeFloat float64) map[string]interface{} {
	return map[string]interface{}{
		"max_plus_one":   maxInt + 1,
		"min_minus_one":  minInt - 1,
		"float_overflow": largeFloat * 2,
		"division_test":  float64(maxInt) / float64(minInt),
	}
}

func (_ testTask) SlowIncrementerTask(initial int, steps int, delayMs int) int {
	current := initial
	for i := 0; i < steps; i++ {
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
		current++
	}
	return current
}

// Tasks for testing concurrency edge cases
func (_ testTask) SharedStateSimulator(id int, iterations int) []int {
	results := make([]int, iterations)
	for i := 0; i < iterations; i++ {
		// Simulate some computation
		time.Sleep(1 * time.Millisecond)
		results[i] = id*1000 + i
	}
	return results
}

func (_ testTask) MemoryLeakTest(allocations int) int {
	// Allocate and release memory in a loop
	totalAllocated := 0
	for i := 0; i < allocations; i++ {
		data := make([]byte, 1024) // 1KB each
		data[0] = byte(i % 256)    // Use the data
		totalAllocated += len(data)
		// Let data go out of scope to be GC'd
	}
	return totalAllocated
}

// Actor for testing actor edge cases
type EdgeCaseActor struct {
	state       map[string]interface{}
	callHistory []string
	errorCount  int
}

func (_ actorFactories) NewEdgeCaseActor() *EdgeCaseActor {
	return &EdgeCaseActor{
		state:       make(map[string]interface{}),
		callHistory: make([]string, 0),
		errorCount:  0,
	}
}

func (a *EdgeCaseActor) StoreNil(key string) {
	a.callHistory = append(a.callHistory, fmt.Sprintf("store_nil_%s", key))
	a.state[key] = nil
}

func (a *EdgeCaseActor) GetStoredValue(key string) interface{} {
	a.callHistory = append(a.callHistory, fmt.Sprintf("get_%s", key))
	return a.state[key]
}

func (a *EdgeCaseActor) ProcessLargeArray(size int) int {
	a.callHistory = append(a.callHistory, fmt.Sprintf("process_array_%d", size))
	data := make([]int, size)
	sum := 0
	for i := range data {
		data[i] = i
		sum += i
	}
	return sum
}

func (a *EdgeCaseActor) InterruptibleOperation(duration int) string {
	a.callHistory = append(a.callHistory, fmt.Sprintf("interruptible_%d", duration))

	// Simulate work that can be interrupted
	for i := 0; i < duration; i++ {
		time.Sleep(10 * time.Millisecond)
		// In a real scenario, we might check for cancellation signals here
	}

	return fmt.Sprintf("completed_%d_steps", duration)
}

func (a *EdgeCaseActor) GetCallHistory() []string {
	return a.callHistory
}

func (a *EdgeCaseActor) ClearHistory() {
	a.callHistory = make([]string, 0)
}

func (a *EdgeCaseActor) SimulateError(shouldError bool) string {
	a.callHistory = append(a.callHistory, fmt.Sprintf("error_sim_%t", shouldError))
	if shouldError {
		a.errorCount++
		panic(fmt.Sprintf("Simulated error #%d", a.errorCount))
	}
	return "no_error"
}

// Actor for testing resource cleanup
type ResourceCleanupActor struct {
	resources []string
	isActive  bool
}

func (_ actorFactories) NewResourceCleanupActor() *ResourceCleanupActor {
	return &ResourceCleanupActor{
		resources: make([]string, 0),
		isActive:  true,
	}
}

func (a *ResourceCleanupActor) AllocateResource(name string) string {
	if !a.isActive {
		return "actor_inactive"
	}
	a.resources = append(a.resources, name)
	return fmt.Sprintf("allocated_%s", name)
}

func (a *ResourceCleanupActor) GetResourceCount() int {
	return len(a.resources)
}

func (a *ResourceCleanupActor) ReleaseAllResources() int {
	count := len(a.resources)
	a.resources = make([]string, 0)
	return count
}

func (a *ResourceCleanupActor) Deactivate() {
	a.isActive = false
}

func init() {
	AddTestCase("TestEmptyAndNilInputs", func(assert *require.Assertions) {
		// Test empty slice
		ref1 := ray.RemoteCall("EmptySliceTask", []int{})
		result1, err := ref1.GetAll()
		assert.NoError(err)
		assert.Equal(0, result1[0])

		// Test nil map (this might fail depending on gob encoding)
		ref2 := ray.RemoteCall("NilMapTask", map[string]int(nil))
		result2, err := ref2.GetAll()
		assert.NoError(err)
		assert.Equal(-1, result2[0])
	})

	AddTestCase("TestZeroValues", func(assert *require.Assertions) {
		ref := ray.RemoteCall("ZeroValueTask", "", 0, 0.0, false)
		result, err := ref.GetAll()
		assert.NoError(err)

		resultMap := result[0].(map[string]interface{})
		assert.Equal(true, resultMap["string_empty"])
		assert.Equal(true, resultMap["int_zero"])
		assert.Equal(true, resultMap["float_zero"])
		assert.Equal(true, resultMap["bool_false"])
	})

	AddTestCase("TestLargeDataTransfer", func(assert *require.Assertions) {
		// Test transferring large strings
		ref := ray.RemoteCall("LargeStringTask", 1024*100) // 100KB string
		result, err := ref.GetAll()
		assert.NoError(err)

		resultStr := result[0].(string)
		assert.Equal(1024*100, len(resultStr))
		assert.Equal(uint8('A'), resultStr[0])
		assert.Equal(uint8('Z'), resultStr[25])
	})

	AddTestCase("TestDeeplyNestedStructures", func(assert *require.Assertions) {
		ref := ray.RemoteCall("DeepNestedStructTask")
		result, err := ref.GetAll()
		assert.NoError(err)

		resultMap := result[0].(map[string]interface{})
		level1 := resultMap["level1"].(map[any]interface{})
		level2 := level1["level2"].(map[any]interface{})
		level3 := level2["level3"].(map[any]interface{})
		level4 := level3["level4"].(map[any]interface{})

		assert.Equal("deeply nested", level4["info"])
		dataSlice := level4["data"].([]any)
		tools.DeepEqualValues([]any{1, 2, 3, 4, 5}, dataSlice)
	})

	AddTestCase("TestNumericBoundaries", func(assert *require.Assertions) {
		ref := ray.RemoteCall("NumericBoundaryTask", 2147483647, -2147483648, 1.7976931348623157e+308)
		result, err := ref.GetAll()
		assert.NoError(err)

		resultMap := result[0].(map[string]interface{})
		// These operations might overflow, but should not crash
		assert.NotNil(resultMap["max_plus_one"])
		assert.NotNil(resultMap["min_minus_one"])
	})

	AddTestCase("TestConcurrentSlowTasks", func(assert *require.Assertions) {
		// Launch multiple slow tasks concurrently
		var refs []ray.ObjectRef
		numTasks := 5

		for i := 0; i < numTasks; i++ {
			ref := ray.RemoteCall("SlowIncrementerTask", i*10, 5, 20) // 5 steps, 20ms each
			refs = append(refs, ref)
		}

		// Wait for some to complete
		ready, notReady, err := ray.Wait(refs, 3, ray.Option("timeout", 2.0))
		assert.NoError(err)
		assert.Len(ready, 3)
		assert.Len(notReady, 2)

		// Get results from ready tasks
		for _, ref := range ready {
			result, err := ref.GetAll()
			assert.NoError(err)
			assert.IsType(0, result[0])
		}

		// Cancel remaining tasks
		for _, ref := range notReady {
			ref.Cancel()
		}
	})

	AddTestCase("TestMemoryLeakDetection", func(assert *require.Assertions) {
		// Run multiple memory allocation tasks
		var refs []ray.ObjectRef

		for i := 0; i < 10; i++ {
			ref := ray.RemoteCall("MemoryLeakTest", 100) // 100 * 1KB allocations
			refs = append(refs, ref)
		}

		// All should complete successfully
		for _, ref := range refs {
			result, err := ref.GetAll()
			assert.NoError(err)
			assert.Equal(102400, result[0]) // 100 * 1024 bytes
		}
	})

	AddTestCase("TestActorEdgeCases", func(assert *require.Assertions) {
		return // todo
		actor := ray.NewActor("NewEdgeCaseActor")

		// Test storing nil values
		actor.RemoteCall("StoreNil", "test_key").GetAll()

		ref := actor.RemoteCall("GetStoredValue", "test_key")
		result, err := ref.GetAll()
		assert.NoError(err)
		assert.Nil(result[0])

		// Test large array processing
		ref2 := actor.RemoteCall("ProcessLargeArray", 10000)
		sum, err := ref2.GetAll()
		assert.NoError(err)
		assert.Equal(49995000, sum[0]) // sum of 0 to 9999
	})

	AddTestCase("TestActorInterruption", func(assert *require.Assertions) {
		actor := ray.NewActor("NewEdgeCaseActor")

		// Start a long-running operation
		ref := actor.RemoteCall("InterruptibleOperation", 50) // 1 second operation
		err := ref.Cancel()
		assert.NoError(err)
		// Should not be cancelled
		_, err2 := ref.GetAll()
		assert.ErrorIs(err2, ray.ErrCancelled)
	})

	AddTestCase("TestActorErrorRecovery", func(assert *require.Assertions) {
		actor := ray.NewActor("NewEdgeCaseActor")

		// Call that should succeed
		ref1 := actor.RemoteCall("SimulateError", false)
		result1, err1 := ref1.GetAll()
		assert.Nil(err1)
		assert.Equal("no_error", result1[0])

		// Call that should fail
		ref2 := actor.RemoteCall("SimulateError", true)
		_, err2 := ref2.GetAll()
		assert.NotNil(err2)

		// Actor should still be responsive after error
		ref3 := actor.RemoteCall("GetCallHistory")
		history, err3 := ref3.GetAll()
		assert.Nil(err3)
		historySlice := history[0].([]string)
		assert.Contains(historySlice, "error_sim_false")
		assert.Contains(historySlice, "error_sim_true")
	})

	AddTestCase("TestActorResourceCleanup", func(assert *require.Assertions) {
		actor := ray.NewActor("NewResourceCleanupActor")

		// Allocate some resources
		actor.RemoteCall("AllocateResource", "resource1").GetAll()
		actor.RemoteCall("AllocateResource", "resource2").GetAll()
		actor.RemoteCall("AllocateResource", "resource3").GetAll()

		// Check resource count
		ref1 := actor.RemoteCall("GetResourceCount")
		count1, err := ref1.GetAll()
		assert.NoError(err)
		assert.Equal(3, count1[0])

		// Release all resources
		ref2 := actor.RemoteCall("ReleaseAllResources")
		released, err := ref2.GetAll()
		assert.NoError(err)
		assert.Equal(3, released[0])

		// Check count is now zero
		ref3 := actor.RemoteCall("GetResourceCount")
		count2, err := ref3.GetAll()
		assert.NoError(err)
		assert.Equal(0, count2[0])

		// Kill actor to test cleanup
		actor.Kill()
	})

	AddTestCase("TestRapidActorCreationDestruction", func(assert *require.Assertions) {
		// Create and destroy multiple actors rapidly
		for i := 0; i < 10; i++ {
			actor := ray.NewActor("NewEdgeCaseActor")

			// Use the actor briefly
			ref := actor.RemoteCall("GetCallHistory")
			_, err := ref.GetAll()
			assert.NoError(err)

			// Kill the actor
			killErr := actor.Kill()
			assert.Nil(killErr)
		}
	})

	AddTestCase("TestObjectRefAfterTimeout", func(assert *require.Assertions) {
		// Create a slow task
		ref := ray.RemoteCall("SlowIncrementerTask", 0, 10, 100) // 1 second total

		// Try with short timeout first
		_, err1 := ref.GetAll(0.1)
		assert.ErrorIs(err1, ray.ErrTimeout)

		// The ObjectRef should still be valid and we can wait longer
		result, err2 := ref.GetAll(2.0)
		assert.Nil(err2)
		assert.Equal([]interface{}{10}, result)
	})

	AddTestCase("TestMixedSuccessFailureBatch", func(assert *require.Assertions) {
		// Create a mix of tasks that will succeed and fail
		var refs []ray.ObjectRef

		refs = append(refs, ray.RemoteCall("QuickTask", 1))           // Success
		refs = append(refs, ray.RemoteCall("DivideByZero", 10, 0))    // Failure
		refs = append(refs, ray.RemoteCall("QuickTask", 2))           // Success
		refs = append(refs, ray.RemoteCall("TaskThatPanics", "test")) // Failure
		refs = append(refs, ray.RemoteCall("QuickTask", 3))           // Success

		// Wait for all to complete (including failures)
		ready, notReady, err := ray.Wait(refs, 5, ray.Option("timeout", 5.0))
		assert.NoError(err)
		assert.Len(ready, 5)
		assert.Len(notReady, 0)

		// Check that we have the expected mix of success/failure
		successCount := 0
		failureCount := 0

		for _, ref := range ready {
			_, err := ref.GetAll()
			if err != nil {
				failureCount++
			} else {
				successCount++
			}
		}

		assert.Equal(3, successCount)
		assert.Equal(2, failureCount)
	})
}
