package cases

import (
	"fmt"
	"time"

	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

// Test nested remote calls - calling remote tasks/actors from within tasks/actors

// Task that calls other remote tasks
func (_ testTask) CoordinatorTask(taskCount int) []int {
	var refs []ray.ObjectRef

	// Launch multiple remote tasks from within this task
	for i := 0; i < taskCount; i++ {
		ref := ray.RemoteCall("WorkerTask", i, i*10)
		refs = append(refs, ref)
	}

	// Collect results
	var results []int
	for _, ref := range refs {
		result, err := ref.Get1()
		if err != nil {
			panic(fmt.Sprintf("Error getting result: %v", err))
		}
		results = append(results, result.(int))
	}

	return results
}

func (_ testTask) WorkerTask(id, multiplier int) int {
	return id * multiplier
}

// Task that creates and uses actors
func (_ testTask) ActorCoordinatorTask(actorName string, operations []string) []int {
	// Get or create actor from within task
	actor, err := ray.GetActor(actorName)
	if err != nil {
		// Actor doesn't exist, this will fail in our test setup
		// We'll handle this in the test
		panic(fmt.Sprintf("Actor not found: %v", err))
	}

	var results []int
	for _, op := range operations {
		var ref ray.ObjectRef
		switch op {
		case "incr":
			ref = actor.RemoteCall("Incr", 1)
		case "decr":
			ref = actor.RemoteCall("Decr", 1)
		default:
			continue
		}

		result, err := ref.Get1()
		if err != nil {
			panic(fmt.Sprintf("Error calling actor method: %v", err))
		}
		results = append(results, result.(int))
	}

	return results
}

// Task that chains multiple remote calls
func (_ testTask) ChainedTask(initial int, depth int) int {
	if depth <= 0 {
		return initial
	}

	// Call another task recursively
	ref := ray.RemoteCall("ChainedTask", initial*2, depth-1)
	result, err := ref.Get1()
	if err != nil {
		panic(fmt.Sprintf("Error in chained call: %v", err))
	}

	return result.(int)
}

// Task that uses Put and Get operations
func (_ testTask) DataSharingTask(data []int) int {
	// Put data into object store from within task
	objRef, err := ray.Put(data)
	if err != nil {
		panic(fmt.Sprintf("Error putting data: %v", err))
	}

	// Use the stored data in another task
	ref := ray.RemoteCall("ProcessStoredData", objRef)
	result, err := ref.Get1()
	if err != nil {
		panic(fmt.Sprintf("Error processing stored data: %v", err))
	}

	return result.(int)
}

func (_ testTask) ProcessStoredData(data []int) int {
	sum := 0
	for _, v := range data {
		sum += v
	}
	return sum
}

// Task that uses Wait functionality
func (_ testTask) BatchCoordinatorTask(batchSize int) []int {
	var refs []ray.ObjectRef

	// Launch batch of tasks
	for i := 0; i < batchSize; i++ {
		ref := ray.RemoteCall("SlowWorkerTask", i, 100) // 100ms delay
		refs = append(refs, ref)
	}

	// Wait for half to complete
	ready, notReady, err := ray.Wait(refs, batchSize/2, ray.Option("timeout", 1.0))
	if err != nil {
		panic(fmt.Sprintf("Error waiting for tasks: %v", err))
	}

	// Get results from ready tasks
	var results []int
	for _, ref := range ready {
		result, err := ref.Get1()
		if err != nil {
			panic(fmt.Sprintf("Error getting ready result: %v", err))
		}
		results = append(results, result.(int))
	}

	// Cancel remaining tasks
	for _, ref := range notReady {
		ref.Cancel()
	}

	return results
}

func (_ testTask) SlowWorkerTask(id, delayMs int) int {
	time.Sleep(time.Duration(delayMs) * time.Millisecond)
	return id * 10
}

// Actor that calls remote tasks
type TaskCallingActor struct {
	id int
}

func NewTaskCallingActor(id int) *TaskCallingActor {
	return &TaskCallingActor{id: id}
}

func (a *TaskCallingActor) CallRemoteTask(taskName string, value int) int {
	ref := ray.RemoteCall(taskName, value)
	result, err := ref.Get1()
	if err != nil {
		panic(fmt.Sprintf("Actor failed to call remote task: %v", err))
	}
	return result.(int)
}

func (a *TaskCallingActor) BatchCallTasks(count int) []int {
	var refs []ray.ObjectRef

	for i := 0; i < count; i++ {
		ref := ray.RemoteCall("QuickTask", i)
		refs = append(refs, ref)
	}

	var results []int
	for _, ref := range refs {
		result, err := ref.Get1()
		if err != nil {
			panic(fmt.Sprintf("Actor batch call failed: %v", err))
		}
		results = append(results, result.(int))
	}

	return results
}

// Actor that calls other actors
type ActorCallingActor struct {
	id int
}

func NewActorCallingActor(id int) *ActorCallingActor {
	return &ActorCallingActor{id: id}
}

func (a *ActorCallingActor) CallOtherActor(actorName string, methodName string, value int) int {
	actor, err := ray.GetActor(actorName)
	if err != nil {
		panic(fmt.Sprintf("Failed to get actor %s: %v", actorName, err))
	}

	ref := actor.RemoteCall(methodName, value)
	result, err := ref.Get1()
	if err != nil {
		panic(fmt.Sprintf("Failed to call other actor: %v", err))
	}

	return result.(int)
}

func init() {
	// Register actors for nested call tests
	taskCallingActorName := RegisterActor(NewTaskCallingActor)
	actorCallingActorName := RegisterActor(NewActorCallingActor)
	otherActorName := RegisterActor(NewActor)

	AddTestCase("TestNestedRemoteCalls", func(assert *require.Assertions) {
		// Test task calling other remote tasks
		ref := ray.RemoteCall("CoordinatorTask", 5)
		results, err := ref.Get1()
		assert.NoError(err)

		resultSlice := results.([]int)
		assert.Len(resultSlice, 5)

		// Check expected results: [0*0, 1*10, 2*20, 3*30, 4*40] = [0, 10, 40, 90, 160]
		assert.Equal(0, resultSlice[0])
		assert.Equal(10, resultSlice[1])
		assert.Equal(40, resultSlice[2])
		assert.Equal(90, resultSlice[3])
		assert.Equal(160, resultSlice[4])
	})

	AddTestCase("TestChainedRemoteCalls", func(assert *require.Assertions) {
		// Test recursive remote calls
		ref := ray.RemoteCall("ChainedTask", 1, 3)
		result, err := ref.Get1()
		assert.NoError(err)

		// 1 -> 2 -> 4 -> 8 (depth 3)
		assert.Equal(8, result)
	})

	AddTestCase("TestDataSharingInTasks", func(assert *require.Assertions) {
		// Test Put/Get operations within tasks
		data := []int{1, 2, 3, 4, 5}
		ref := ray.RemoteCall("DataSharingTask", data)
		result, err := ref.Get1()
		assert.NoError(err)

		assert.Equal(15, result) // sum of 1+2+3+4+5
	})

	AddTestCase("TestBatchCoordinatorWithWait", func(assert *require.Assertions) {
		// Test Wait functionality within tasks
		ref := ray.RemoteCall("BatchCoordinatorTask", 6)
		results, err := ref.Get1()
		assert.NoError(err)

		resultSlice := results.([]int)
		assert.Len(resultSlice, 3) // Should get 3 results (half of 6)

		// Results should be some subset of [0, 10, 20, 30, 40, 50]
		for _, result := range resultSlice {
			assert.True(result >= 0 && result <= 50 && result%10 == 0)
		}
	})

	AddTestCase("TestActorCallingTasks", func(assert *require.Assertions) {
		// Test actor calling remote tasks
		actor := ray.NewActor(taskCallingActorName, 1)

		ref := actor.RemoteCall("CallRemoteTask", "QuickTask", 5)
		result, err := ref.Get1()
		assert.NoError(err)
		assert.Equal(10, result) // QuickTask returns id * 2 = 5 * 2 = 10
	})

	AddTestCase("TestActorBatchCallingTasks", func(assert *require.Assertions) {
		// Test actor calling multiple remote tasks
		actor := ray.NewActor(taskCallingActorName, 2)

		ref := actor.RemoteCall("BatchCallTasks", 3)
		results, err := ref.Get1()
		assert.NoError(err)

		resultSlice := results.([]int)
		assert.Len(resultSlice, 3)
		assert.Equal(0, resultSlice[0]) // QuickTask(0) = 0 * 2 = 0
		assert.Equal(2, resultSlice[1]) // QuickTask(1) = 1 * 2 = 2
		assert.Equal(4, resultSlice[2]) // QuickTask(2) = 2 * 2 = 4
	})

	AddTestCase("TestActorCallingOtherActors", func(assert *require.Assertions) {
		// Create two actors
		callingActor := ray.NewActor(actorCallingActorName, 1)

		// Create a named counter actor
		_ = ray.NewActor(otherActorName, 100, ray.Option("name", "test_counter"))

		// Use the calling actor to interact with the counter actor
		ref := callingActor.RemoteCall("CallOtherActor", "test_counter", "Incr", 5)
		result, err := ref.Get1()
		assert.NoError(err)
		assert.Equal(105, result) // 100 + 5 = 105
	})

	AddTestCase("TestNestedActorCoordination", func(assert *require.Assertions) {
		// Create a named actor first
		ray.NewActor(otherActorName, 50, ray.Option("name", "coord_counter"))

		// Test task coordinating with named actor
		operations := []string{"incr", "incr", "decr", "incr"}
		ref := ray.RemoteCall("ActorCoordinatorTask", "coord_counter", operations)
		results, err := ref.Get1()
		assert.NoError(err)

		resultSlice := results.([]int)
		assert.Len(resultSlice, 4)
		// Starting from 50: 51, 52, 51, 52
		assert.Equal(51, resultSlice[0])
		assert.Equal(52, resultSlice[1])
		assert.Equal(51, resultSlice[2])
		assert.Equal(52, resultSlice[3])
	})
}
