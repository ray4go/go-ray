package main

import (
	"github.com/ray4go/go-ray/ray"
	"fmt"
	"github.com/stretchr/testify/require"
	"time"
)

// Test error handling and edge cases
func (_ testTask) TaskThatPanics(message string) string {
	panic("intentional panic: " + message)
}

func (_ testTask) DivideByZero(a, b int) int {
	return a / b
}

func (_ testTask) TaskWithDelay(delay int, value string) string {
	time.Sleep(time.Duration(delay) * time.Millisecond)
	return fmt.Sprintf("delayed_%s", value)
}

func (_ testTask) TaskThatReturnsError(shouldFail bool) (string, error) {
	if shouldFail {
		return "", fmt.Errorf("task failed as requested")
	}
	return "success", nil
}

func (_ testTask) ProcessLargeData(size int) []int {
	data := make([]int, size)
	for i := 0; i < size; i++ {
		data[i] = i * i
	}
	return data
}

func (_ testTask) EmptyReturns() {
	// Task with no return values
}

func (_ testTask) MultipleReturnValues(a, b int) (int, int, string, bool) {
	return a + b, a * b, fmt.Sprintf("%d_%d", a, b), a > b
}

func init() {
	addTestCase("TestPanicHandling", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("TaskThatPanics", "test_panic")
		_, err := objRef.Get1()

		// The task should fail but not crash the system
		assert.NotNil(err)
		// Note: The exact error handling behavior may depend on implementation
	})

	addTestCase("TestDivisionByZero", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("DivideByZero", 10, 0)
		_, err := objRef.Get1()

		// Should handle division by zero gracefully
		assert.NotNil(err)
	})

	addTestCase("TestTimeoutBehavior", func(assert *require.Assertions) {
		// Create a task that takes longer than our timeout
		objRef := ray.RemoteCall("TaskWithDelay", 500, "test")

		// Try to get result with short timeout
		_, err := objRef.GetAllTimeout(0.1) // 100ms timeout

		assert.ErrorIs(err, ray.ErrTimeout)
	})

	addTestCase("TestCancelAfterCompletion", func(assert *require.Assertions) {
		// Create a fast task
		objRef := ray.RemoteCall("TaskWithDelay", 10, "fast")

		// Wait for completion
		result, err := objRef.Get1()
		assert.Nil(err)
		assert.Equal("delayed_fast", result)

		// Try to cancel after completion (should be safe)
		cancelErr := objRef.Cancel()
		// Cancel might succeed or fail depending on timing, both are valid
		_ = cancelErr // Don't assert on this
	})

	addTestCase("TestLargeDataProcessing", func(assert *require.Assertions) {
		// Test with reasonably large data
		size := 10000
		objRef := ray.RemoteCall("ProcessLargeData", size)

		result, err := objRef.Get1()
		assert.Nil(err)

		dataSlice := result.([]int)
		assert.Len(dataSlice, size)
		assert.Equal(0, dataSlice[0])     // 0*0 = 0
		assert.Equal(1, dataSlice[1])     // 1*1 = 1
		assert.Equal(4, dataSlice[2])     // 2*2 = 4
		assert.Equal(9801, dataSlice[99]) // 99*99 = 9801
	})

	addTestCase("TestEmptyReturnValues", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("EmptyReturns")

		// Test Get0 for no return values
		err := objRef.Get0()
		assert.Nil(err)

		// Test GetAll for empty returns
		results, err := objRef.GetAll()
		assert.Nil(err)
		assert.Empty(results)
	})

	addTestCase("TestMultipleReturnValues", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("MultipleReturnValues", 15, 10)

		// Test getting all values at once
		results, err := objRef.GetAll()
		assert.Nil(err)
		assert.Len(results, 4)
		assert.Equal(25, results[0])      // 15 + 10
		assert.Equal(150, results[1])     // 15 * 10
		assert.Equal("15_10", results[2]) // formatted string
		assert.Equal(true, results[3])    // 15 > 10

		// Test specific Get methods
		objRef2 := ray.RemoteCall("MultipleReturnValues", 5, 8)
		val1, val2, val3, val4, err := objRef2.Get4()
		assert.Nil(err)
		assert.Equal(13, val1)    // 5 + 8
		assert.Equal(40, val2)    // 5 * 8
		assert.Equal("5_8", val3) // formatted string
		assert.Equal(false, val4) // 5 > 8 is false
	})

	addTestCase("TestInvalidTaskName", func(assert *require.Assertions) {
		// This should panic since the task doesn't exist
		assert.Panics(func() {
			ray.RemoteCall("NonExistentTask", 1, 2, 3)
		})
	})

	addTestCase("TestInvalidArgumentCount", func(assert *require.Assertions) {
		// This should panic due to wrong number of arguments
		assert.Panics(func() {
			ray.RemoteCall("MultipleReturnValues", 1) // needs 2 args, only providing 1
		})
	})

	addTestCase("TestWaitWithTimeout", func(assert *require.Assertions) {
		// Create a slow task
		slowRef := ray.RemoteCall("TaskWithDelay", 1000, "slow") // 1 second delay

		refs := []ray.ObjectRef{slowRef}

		// Wait with short timeout
		ready, notReady, err := ray.Wait(refs, ray.NewOption("timeout", 0.1)) // 100ms timeout

		assert.Nil(err)
		assert.Empty(ready)
		assert.Len(notReady, 1)
		assert.Contains(notReady, slowRef)
	})

	addTestCase("TestMultipleCancellations", func(assert *require.Assertions) {
		// Create multiple slow tasks
		ref1 := ray.RemoteCall("TaskWithDelay", 2000, "task1")
		ref2 := ray.RemoteCall("TaskWithDelay", 2000, "task2")
		ref3 := ray.RemoteCall("TaskWithDelay", 2000, "task3")

		// Cancel all tasks
		err1 := ref1.Cancel()
		err2 := ref2.Cancel()
		err3 := ref3.Cancel()

		// All cancellations should succeed (or at least not crash)
		assert.Nil(err1)
		assert.Nil(err2)
		assert.Nil(err3)

		// Getting results should return cancelled errors
		_, getErr1 := ref1.Get1()
		_, getErr2 := ref2.Get1()
		_, getErr3 := ref3.Get1()

		assert.ErrorIs(getErr1, ray.ErrCancelled)
		assert.ErrorIs(getErr2, ray.ErrCancelled)
		assert.ErrorIs(getErr3, ray.ErrCancelled)
	})
}
