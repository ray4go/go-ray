package cases

import (
	"time"

	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

// Test concurrent execution and ObjectRef chaining
func (TestTask) SlowAdd(a, b int) int {
	time.Sleep(100 * time.Millisecond)
	return a + b
}

func (TestTask) SlowMultiply(a, b int) int {
	time.Sleep(150 * time.Millisecond)
	return a * b
}

func (TestTask) CombineResults(x, y, z int) int {
	return x + y + z
}

func (TestTask) ComputeSum(numbers []int) int {
	sum := 0
	for _, n := range numbers {
		sum += n
	}
	return sum
}

func (TestTask) GenerateSequence(start, end int) []int {
	result := make([]int, 0, end-start+1)
	for i := start; i <= end; i++ {
		result = append(result, i)
	}
	return result
}

func init() {
	AddTestCase("TestConcurrentExecution", func(assert *require.Assertions) {
		// warn up
		_, err := ray.RemoteCall("SlowAdd", 10, 20).GetAll()
		assert.NoError(err)

		start := time.Now()

		// Launch multiple tasks concurrently
		ref1 := ray.RemoteCall("SlowAdd", 10, 20)
		ref2 := ray.RemoteCall("SlowMultiply", 5, 6)
		ref3 := ray.RemoteCall("SlowAdd", 100, 200)

		// Get results
		result1, err1 := ray.Get1[int](ref1)
		result2, err2 := ray.Get1[int](ref2)
		result3, err3 := ray.Get1[int](ref3)

		elapsed := time.Since(start)

		assert.Nil(err1)
		assert.Nil(err2)
		assert.Nil(err3)
		assert.Equal(30, result1)
		assert.Equal(30, result2)
		assert.Equal(300, result3)

		// Should be faster than sequential execution
		// Sequential would take ~3500ms, concurrent should be ~150ms
		assert.Less(elapsed, 1800*time.Millisecond)
	})

	AddTestCase("TestObjectRefChaining", func(assert *require.Assertions) {
		// Create a chain of dependent tasks
		ref1 := ray.RemoteCall("SlowAdd", 5, 10)        // = 15
		ref2 := ray.RemoteCall("SlowMultiply", ref1, 2) // = 30
		ref3 := ray.RemoteCall("SlowAdd", ref2, 5)      // = 35
		ref4 := ray.RemoteCall("SlowMultiply", ref3, 3) // = 105

		result, err := ray.Get1[int](ref4)
		assert.NoError(err)
		// (5+10) = 15, 15*2 = 30, 30+5 = 35, 35*3 = 105
		assert.Equal(105, result)
	})

	AddTestCase("TestComplexObjectRefChaining", func(assert *require.Assertions) {
		// Create multiple ObjectRefs and combine them
		ref1 := ray.RemoteCall("SlowAdd", 1, 2)      // = 3
		ref2 := ray.RemoteCall("SlowMultiply", 4, 5) // = 20
		ref3 := ray.RemoteCall("SlowAdd", 10, 15)    // = 25

		// Combine all three results
		combinedRef := ray.RemoteCall("CombineResults", ref1, ref2, ref3)
		result, err := ray.Get1[int](combinedRef)

		assert.NoError(err)
		assert.Equal(48, result) // 3 + 20 + 25 = 48
	})

	AddTestCase("TestObjectRefWithCollections", func(assert *require.Assertions) {
		// Generate a sequence using ObjectRef
		seqRef := ray.RemoteCall("GenerateSequence", 1, 10)

		// Use the sequence result in another task
		sumRef := ray.RemoteCall("ComputeSum", seqRef)

		result, err := ray.Get1[int](sumRef)
		assert.NoError(err)
		assert.Equal(55, result) // 1+2+3+...+10 = 55
	})

	AddTestCase("TestWaitFunctionality", func(assert *require.Assertions) {
		ray.RemoteCall("SlowAdd", 1, 1).GetAll() // warn up

		// Create multiple tasks with different completion times
		fastRef := ray.RemoteCall("SlowAdd", 1, 1)      // ~100ms
		slowRef := ray.RemoteCall("SlowMultiply", 2, 2) // ~150ms
		mediumRef := ray.RemoteCall("SlowAdd", 3, 3)    // ~100ms

		refs := []*ray.ObjectRef{fastRef, slowRef, mediumRef}

		// Wait for at least 2 tasks to complete
		ready, notReady, err := ray.Wait(refs, 2, ray.Option("timeout", 1.0))

		assert.NoError(err)
		assert.Equal(len(refs), len(ready)+len(notReady))
		assert.Len(ready, 2)
		assert.Len(notReady, 1)

		// The slow task should still be running
		//assert.Contains(notReady, slowRef)
	})

	AddTestCase("TestWaitAll", func(assert *require.Assertions) {
		// Create tasks and wait for all to complete
		ref1 := ray.RemoteCall("SlowAdd", 10, 20)
		ref2 := ray.RemoteCall("SlowAdd", 30, 40)

		refs := []*ray.ObjectRef{ref1, ref2}

		// Wait for all tasks
		ready, notReady, err := ray.Wait(refs, 2, ray.Option("timeout", 2.0))

		assert.NoError(err)
		assert.Len(ready, 2)
		assert.Len(notReady, 0)

		// Verify results
		result1, err1 := ray.Get1[int](ref1)
		result2, err2 := ray.Get1[int](ref2)

		assert.Nil(err1)
		assert.Nil(err2)
		assert.Equal(30, result1)
		assert.Equal(70, result2)
	})
}
