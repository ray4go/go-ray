package cases

import (
	"fmt"
	"time"

	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

// Test ray task options and configuration
func (TestTask) CpuIntensiveTask(iterations int) int {
	// Simulate CPU-intensive work
	result := 0
	for i := 0; i < iterations; i++ {
		for j := 0; j < 1000; j++ {
			result += i * j
		}
	}
	return result
}

func (TestTask) MemoryIntensiveTask(size int) []int {
	// Simulate memory-intensive work
	data := make([]int, size)
	for i := range data {
		data[i] = i * i
	}
	return data
}

func (TestTask) ResourceTrackingTask(taskId string, duration int) string {
	time.Sleep(time.Duration(duration) * time.Millisecond)
	return "task_" + taskId + "_completed"
}

func (TestTask) BatchTask(batchId int, items []string) []string {
	result := make([]string, len(items))
	for i, item := range items {
		result[i] = item + "_batch_" + fmt.Sprint(batchId)
	}
	return result
}

func init() {
	AddTestCase("TestTaskWithCpuOption", func(assert *require.Assertions) {
		// Test task with CPU resource specification
		objRef := ray.RemoteCall("CpuIntensiveTask", 1000,
			ray.Option("num_cpus", 2))

		result, err := ray.Get1[int](objRef)
		assert.NoError(err)
		assert.IsType(0, result) // Should return an integer

		// The actual computation result depends on the algorithm
		// Just verify it's a reasonable number
		assert.Greater(result, 0)
	})

	AddTestCase("TestTaskWithMemoryOption", func(assert *require.Assertions) {
		// Test task with memory resource specification
		objRef := ray.RemoteCall("MemoryIntensiveTask", 10000,
			ray.Option("memory", 100*1024*1024)) // 100MB

		result, err := ray.Get1[[]int](objRef)
		assert.NoError(err)

		assert.Len(result, 10000)
		assert.Equal(0, result[0])            // 0*0 = 0
		assert.Equal(9999*9999, result[9999]) // last element
	})

	AddTestCase("TestTaskWithMultipleOptions", func(assert *require.Assertions) {
		// Test task with multiple resource options
		objRef := ray.RemoteCall("ResourceTrackingTask", "multi_option", 100,
			ray.Option("num_cpus", 1),
			ray.Option("memory", 50*1024*1024), // 50MB
		)

		result, err := ray.Get1[string](objRef)
		assert.NoError(err)
		assert.Equal("task_multi_option_completed", result)
	})

	AddTestCase("TestTaskWithRetryOptions", func(assert *require.Assertions) {
		// Test task with retry configuration
		objRef := ray.RemoteCall("ResourceTrackingTask", "retry", 25,
			ray.Option("max_retries", 3),
			ray.Option("retry_exceptions", true))

		result, err := ray.Get1[string](objRef)
		assert.NoError(err)
		assert.Equal("task_retry_completed", result)
	})

	AddTestCase("TestBatchTasksWithDifferentOptions", func(assert *require.Assertions) {
		// Create multiple tasks with different resource requirements
		items1 := []string{"item1", "item2", "item3"}
		items2 := []string{"item4", "item5", "item6"}
		items3 := []string{"item7", "item8", "item9"}

		// Task with low resources
		ref1 := ray.RemoteCall("BatchTask", 1, items1,
			ray.Option("num_cpus", 0.5))

		// Task with medium resources
		ref2 := ray.RemoteCall("BatchTask", 2, items2,
			ray.Option("num_cpus", 1),
			ray.Option("memory", 100*1024*1024))

		// Task with high resources
		ref3 := ray.RemoteCall("BatchTask", 3, items3,
			ray.Option("num_cpus", 2),
			ray.Option("memory", 200*1024*1024))

		// Get all results
		result1, err1 := ray.Get1[[]string](ref1)
		result2, err2 := ray.Get1[[]string](ref2)
		result3, err3 := ray.Get1[[]string](ref3)

		assert.Nil(err1)
		assert.Nil(err2)
		assert.Nil(err3)

		// Verify results
		assert.Equal([]string{"item1_batch_1", "item2_batch_1", "item3_batch_1"}, result1)
		assert.Equal([]string{"item4_batch_2", "item5_batch_2", "item6_batch_2"}, result2)
		assert.Equal([]string{"item7_batch_3", "item8_batch_3", "item9_batch_3"}, result3)
	})

	AddTestCase("TestTaskWithSchedulingOptions", func(assert *require.Assertions) {
		// Test task with scheduling-related options
		objRef := ray.RemoteCall("ResourceTrackingTask", "scheduled", 30,
			ray.Option("scheduling_strategy", "SPREAD"),
			ray.Option("placement_group", nil))

		result, err := ray.Get1[string](objRef)
		assert.NoError(err)
		assert.Equal("task_scheduled_completed", result)
	})

	AddTestCase("TestTaskWithStringOption", func(assert *require.Assertions) {
		// Test task with string-based options
		objRef := ray.RemoteCall("ResourceTrackingTask", "string_opt", 20,
			ray.Option("name", "custom_task_name"),
			ray.Option("runtime_env", map[string]interface{}{
				"env_vars": map[string]string{"TEST_VAR": "test_value"},
			}))

		result, err := ray.Get1[string](objRef)
		assert.NoError(err)
		assert.Equal("task_string_opt_completed", result)
	})

	AddTestCase("TestInvalidTaskOptions", func(assert *require.Assertions) {
		// Test with potentially invalid options (should crash)
		assert.Panicsf(func() {
			ray.RemoteCall("ResourceTrackingTask", "invalid_opt", 10,
				ray.Option("invalid_option", "invalid_value"),
				ray.Option("negative_cpus", -1))
		}, "Invalid option")
	})

	AddTestCase("TestOptionsWithObjectRefs", func(assert *require.Assertions) {
		// Test using task options with ObjectRef arguments
		dataRef, err := ray.Put([]string{"ref_item1", "ref_item2"})
		assert.NoError(err)

		objRef := ray.RemoteCall("BatchTask", 99, dataRef,
			ray.Option("num_cpus", 1),
			ray.Option("memory", 50*1024*1024))

		result, err := ray.Get1[[]string](objRef)
		assert.NoError(err)
		assert.Equal([]string{"ref_item1_batch_99", "ref_item2_batch_99"}, result)
	})
}
