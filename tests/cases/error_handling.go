package cases

import (
	"fmt"
	"time"

	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

// Test error handling and edge cases
func (testTask) TaskThatPanics(message string) string {
	panic("intentional panic: " + message)
}

func (testTask) DivideByZero(a, b int) int {
	return a / b
}

func (testTask) TaskWithDelay(delay int, value string) string {
	time.Sleep(time.Duration(delay) * time.Millisecond)
	return fmt.Sprintf("delayed_%s", value)
}

func (testTask) TaskThatReturnsError(shouldFail bool) (string, error) {
	if shouldFail {
		return "", fmt.Errorf("task failed as requested")
	}
	return "success", nil
}

func (testTask) ProcessLargeData(size int) []int {
	data := make([]int, size)
	for i := 0; i < size; i++ {
		data[i] = i * i
	}
	return data
}

func (testTask) EmptyReturns() {
	// Task with no return values
}

func (testTask) MultipleReturnValues(a, b int) (int, int, string, bool) {
	return a + b, a * b, fmt.Sprintf("%d_%d", a, b), a > b
}

func init() {
	AddTestCase("TestPanicHandling", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("TaskThatPanics", "test_panic")
		_, err := ray.Get1[any](objRef)

		// The task should fail but not crash the system
		assert.NotNil(err)
		// Note: The exact error handling behavior may depend on implementation
	})

	AddTestCase("TestDivisionByZero", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("DivideByZero", 10, 0)
		_, err := ray.Get1[any](objRef)

		// Should handle division by zero gracefully
		assert.NotNil(err)
	})

	AddTestCase("TestTimeoutBehavior", func(assert *require.Assertions) {
		// Create a task that takes longer than our timeout
		objRef := ray.RemoteCall("TaskWithDelay", 500, "test")

		// Try to get result with short timeout
		_, err := objRef.GetAll(ray.WithTimeout(time.Millisecond * 100)) // 100ms timeout

		assert.ErrorIs(err, ray.ErrTimeout)
	})

	AddTestCase("TestCancelAfterCompletion", func(assert *require.Assertions) {
		// Create a fast task
		objRef := ray.RemoteCall("TaskWithDelay", 10, "fast")

		// Wait for completion
		result, err := ray.Get1[string](objRef)
		assert.NoError(err)
		assert.Equal("delayed_fast", result)

		// Try to cancel after completion (should be safe)
		cancelErr := objRef.Cancel()
		// Cancel might succeed or fail depending on timing, both are valid
		_ = cancelErr // Don't assert on this
	})

	AddTestCase("TestLargeDataProcessing", func(assert *require.Assertions) {
		// Test with reasonably large data
		size := 10000
		objRef := ray.RemoteCall("ProcessLargeData", size)

		result, err := ray.Get1[[]int](objRef)
		assert.NoError(err)

		dataSlice := result
		assert.Len(dataSlice, size)
		assert.Equal(0, dataSlice[0])     // 0*0 = 0
		assert.Equal(1, dataSlice[1])     // 1*1 = 1
		assert.Equal(4, dataSlice[2])     // 2*2 = 4
		assert.Equal(9801, dataSlice[99]) // 99*99 = 9801
	})

	AddTestCase("TestEmptyReturnValues", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("EmptyReturns")
		objRef.DisableAutoRelease()

		// Test Get0 for no return values
		err := ray.Get0(objRef)
		assert.NoError(err)

		// Test GetAll for empty returns
		results, err := objRef.GetAll()
		assert.NoError(err)
		assert.Empty(results)
	})

	AddTestCase("TestMultipleReturnValues", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("MultipleReturnValues", 15, 10)

		// Test getting all values at once
		results, err := objRef.GetAll()
		assert.NoError(err)
		assert.Len(results, 4)
		assert.Equal(25, results[0])      // 15 + 10
		assert.Equal(150, results[1])     // 15 * 10
		assert.Equal("15_10", results[2]) // formatted string
		assert.Equal(true, results[3])    // 15 > 10

		// Test specific Get methods
		objRef2 := ray.RemoteCall("MultipleReturnValues", 5, 8)
		val1, val2, val3, val4, err := ray.Get4[int, int, string, bool](objRef2)
		assert.NoError(err)
		assert.Equal(13, val1)    // 5 + 8
		assert.Equal(40, val2)    // 5 * 8
		assert.Equal("5_8", val3) // formatted string
		assert.Equal(false, val4) // 5 > 8 is false
	})

	AddTestCase("TestInvalidTaskName", func(assert *require.Assertions) {
		// This should panic since the task doesn't exist
		assert.Panics(func() {
			ray.RemoteCall("NonExistentTask", 1, 2, 3)
		})
	})

	AddTestCase("TestInvalidArgumentCount", func(assert *require.Assertions) {
		// This should panic due to wrong number of arguments
		assert.Panics(func() {
			ray.RemoteCall("MultipleReturnValues", 1) // needs 2 args, only providing 1
		})
	})

	AddTestCase("TestWaitWithTimeout", func(assert *require.Assertions) {
		// Create a slow task
		slowRef := ray.RemoteCall("TaskWithDelay", 1000, "slow") // 1 second delay

		refs := []*ray.ObjectRef{slowRef}

		// Wait with short timeout
		ready, notReady, err := ray.Wait(refs, 1, ray.Option("timeout", 0.1)) // 100ms timeout

		assert.NoError(err)
		assert.Empty(ready)
		assert.Len(notReady, 1)
		assert.Contains(notReady, slowRef)
	})

	AddTestCase("TestMultipleCancellations", func(assert *require.Assertions) {
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
		_, getErr1 := ray.Get1[string](ref1)
		_, getErr2 := ray.Get1[string](ref2)
		_, getErr3 := ray.Get1[string](ref3)

		assert.ErrorIs(getErr1, ray.ErrCancelled)
		assert.ErrorIs(getErr2, ray.ErrCancelled)
		assert.ErrorIs(getErr3, ray.ErrCancelled)
	})
}
