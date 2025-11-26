package cases

import (
	"time"

	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

// Test performance and scalability scenarios
func (TestTask) QuickTask(id int) int {
	return id * 2
}

func (TestTask) MediumTask(id int, data []int) int {
	sum := id
	for _, v := range data {
		sum += v
	}
	return sum
}

func (TestTask) FibonacciTask(n int) int {
	if n <= 1 {
		return n
	}
	return LocalFibonacciTask(n-1) + LocalFibonacciTask(n-2)
}

func LocalFibonacciTask(n int) int {
	if n <= 1 {
		return n
	}
	if n == 2 {
		return 1
	}

	a, b := 0, 1
	for i := 2; i <= n; i++ {
		a, b = b, a+b
	}
	return b
}

func (TestTask) MatrixMultiplyRow(rowA []float64, matrixB [][]float64) []float64 {
	cols := len(matrixB[0])
	result := make([]float64, cols)

	for j := 0; j < cols; j++ {
		sum := 0.0
		for k := 0; k < len(rowA); k++ {
			sum += rowA[k] * matrixB[k][j]
		}
		result[j] = sum
	}
	return result
}

func (TestTask) AggregateResults(results [][]float64) []float64 {
	if len(results) == 0 {
		return []float64{}
	}

	cols := len(results[0])
	aggregated := make([]float64, cols)

	for _, row := range results {
		for j := 0; j < cols; j++ {
			aggregated[j] += row[j]
		}
	}
	return aggregated
}

func init() {
	AddTestCase("TestHighThroughputTasks", func(assert *require.Assertions) {
		start := time.Now()

		// Launch many small tasks
		numTasks := 100
		refs := make([]*ray.ObjectRef, numTasks)

		for i := 0; i < numTasks; i++ {
			refs[i] = ray.RemoteCall("QuickTask", i)
		}

		// Collect all results
		results := make([]int, numTasks)
		for i, ref := range refs {
			result, err := ray.Get1[int](ref)
			assert.NoError(err)
			results[i] = result
		}

		elapsed := time.Since(start)

		// Verify results
		for i := 0; i < numTasks; i++ {
			assert.Equal(i*2, results[i])
		}

		// Should complete reasonably quickly
		assert.Less(elapsed, 10*time.Second)
	})

	AddTestCase("TestMediumScaleTasks", func(assert *require.Assertions) {
		// Create medium-scale tasks with larger data
		numTasks := 20
		dataSize := 1000

		refs := make([]*ray.ObjectRef, numTasks)

		for i := 0; i < numTasks; i++ {
			// Create test data
			data := make([]int, dataSize)
			for j := 0; j < dataSize; j++ {
				data[j] = j
			}

			refs[i] = ray.RemoteCall("MediumTask", i, data)
		}

		// Collect results
		for i, ref := range refs {
			result, err := ray.Get1[int](ref)
			assert.NoError(err)

			// Expected: i + sum(0 to 999) = i + 499500
			expected := i + 499500
			assert.Equal(expected, result)
		}
	})

	AddTestCase("TestComputeIntensiveTask", func(assert *require.Assertions) {
		// Test CPU-intensive tasks
		start := time.Now()

		// Calculate Fibonacci numbers in parallel
		refs := []*ray.ObjectRef{
			ray.RemoteCall("FibonacciTask", 20),
			ray.RemoteCall("FibonacciTask", 21),
			ray.RemoteCall("FibonacciTask", 22),
			ray.RemoteCall("FibonacciTask", 23),
		}

		results := make([]int, len(refs))
		for i, ref := range refs {
			result, err := ray.Get1[int](ref)
			assert.NoError(err)
			results[i] = result
		}

		elapsed := time.Since(start)

		// Verify Fibonacci results
		assert.Equal(6765, results[0])  // F(20)
		assert.Equal(10946, results[1]) // F(21)
		assert.Equal(17711, results[2]) // F(22)
		assert.Equal(28657, results[3]) // F(23)

		// Should benefit from parallelization
		assert.Less(elapsed, 5*time.Second)
	})

	AddTestCase("TestParallelMatrixComputation", func(assert *require.Assertions) {
		// Test parallel matrix operations
		rows := 10
		cols := 8

		// Create matrices
		matrixA := make([][]float64, rows)
		matrixB := make([][]float64, cols)

		for i := 0; i < rows; i++ {
			matrixA[i] = make([]float64, cols)
			for j := 0; j < cols; j++ {
				matrixA[i][j] = float64(i*cols + j)
			}
		}

		for i := 0; i < cols; i++ {
			matrixB[i] = make([]float64, cols)
			for j := 0; j < cols; j++ {
				matrixB[i][j] = float64(i + j)
			}
		}

		// Process each row in parallel
		rowRefs := make([]*ray.ObjectRef, rows)
		for i := 0; i < rows; i++ {
			rowRefs[i] = ray.RemoteCall("MatrixMultiplyRow", matrixA[i], matrixB)
		}

		// Collect row results
		rowResults := make([][]float64, rows)
		for i, ref := range rowRefs {
			result, err := ray.Get1[[]float64](ref)
			assert.NoError(err)
			rowResults[i] = result
		}

		// Aggregate results
		aggregateRef := ray.RemoteCall("AggregateResults", rowResults)
		finalResult, err := ray.Get1[[]float64](aggregateRef)
		assert.NoError(err)

		aggregated := finalResult
		assert.Len(aggregated, cols)

		// Verify result makes sense (sum should be positive)
		for _, val := range aggregated {
			assert.Greater(val, 0.0)
		}
	})

	AddTestCase("TestMemoryScaling", func(assert *require.Assertions) {
		// Test tasks with increasing memory usage
		sizes := []int{1000, 5000, 10000, 20000}
		refs := make([]*ray.ObjectRef, len(sizes))

		for i, size := range sizes {
			refs[i] = ray.RemoteCall("ProcessLargeData", size)
		}

		// Verify all tasks complete successfully
		for i, ref := range refs {
			result, err := ray.Get1[[]int](ref)
			assert.NoError(err)

			data := result
			assert.Len(data, sizes[i])

			// Verify some values
			assert.Equal(0, data[0])
			assert.Equal(1, data[1])
			if sizes[i] > 10 {
				assert.Equal(100, data[10]) // 10*10 = 100
			}
		}
	})

	AddTestCase("TestTaskChainScaling", func(assert *require.Assertions) {
		// Test long chains of dependent tasks
		chainLength := 10

		// Start with initial value
		currentRef := ray.RemoteCall("QuickTask", 1) // = 2

		// Chain tasks together
		for i := 1; i < chainLength; i++ {
			currentRef = ray.RemoteCall("QuickTask", currentRef) // Each step doubles the value
		}

		result, err := ray.Get1[int](currentRef)
		assert.NoError(err)

		// Each task doubles the input, so after 10 tasks: 1 * 2^10 = 1024
		expected := 1
		for i := 0; i < chainLength; i++ {
			expected *= 2
		}
		assert.Equal(expected, result)
	})

	AddTestCase("TestConcurrentResourceUsage", func(assert *require.Assertions) {
		// Test multiple resource-intensive tasks running concurrently
		start := time.Now()

		// Mix of different task types
		cpuRef1 := ray.RemoteCall("FibonacciTask", 18, ray.Option("num_cpus", 1))
		cpuRef2 := ray.RemoteCall("FibonacciTask", 19, ray.Option("num_cpus", 1))

		memRef1 := ray.RemoteCall("ProcessLargeData", 15000, ray.Option("memory", 100*1024*1024))
		memRef2 := ray.RemoteCall("ProcessLargeData", 12000, ray.Option("memory", 80*1024*1024))

		// Wait for all to complete
		refs := []*ray.ObjectRef{cpuRef1, cpuRef2, memRef1, memRef2}
		ready, notReady, err := ray.Wait(refs, 4, ray.Option("timeout", 30.0))

		assert.NoError(err)
		assert.Len(ready, 4)
		assert.Empty(notReady)

		elapsed := time.Since(start)

		// Should complete faster than sequential execution due to parallelism
		assert.Less(elapsed, 15*time.Second)

		// Verify all results
		for _, ref := range refs {
			_, err := ref.GetAll()
			assert.NoError(err)
		}
	})

	AddTestCase("TestRapidFireTasks", func(assert *require.Assertions) {
		// Test launching tasks in rapid succession
		numRounds := 5
		tasksPerRound := 20

		for round := 0; round < numRounds; round++ {
			refs := make([]*ray.ObjectRef, tasksPerRound)

			// Launch all tasks quickly
			for i := 0; i < tasksPerRound; i++ {
				refs[i] = ray.RemoteCall("QuickTask", round*tasksPerRound+i)
			}

			// Wait for this round to complete
			ready, notReady, err := ray.Wait(refs, tasksPerRound,
				ray.Option("timeout", 5.0))

			assert.NoError(err)
			assert.Len(ready, tasksPerRound)
			assert.Empty(notReady)
		}
	})
}
