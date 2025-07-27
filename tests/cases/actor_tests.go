package cases

import (
	"fmt"
	"time"

	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

// Comprehensive actor tests covering various actor scenarios

// Simple stateful actor for basic tests
type StatefulActor struct {
	value   int
	history []int
}

func NewStatefulActor(initialValue int) *StatefulActor {
	return &StatefulActor{
		value:   initialValue,
		history: make([]int, 0),
	}
}

func (a *StatefulActor) GetValue() int {
	return a.value
}

func (a *StatefulActor) SetValue(newValue int) int {
	oldValue := a.value
	a.value = newValue
	a.history = append(a.history, newValue)
	return oldValue
}

func (a *StatefulActor) Add(delta int) int {
	a.value += delta
	a.history = append(a.history, a.value)
	return a.value
}

func (a *StatefulActor) GetHistory() []int {
	return a.history
}

func (a *StatefulActor) Reset() {
	a.value = 0
	a.history = make([]int, 0)
}

// Actor with complex state
type ComplexActor struct {
	id       string
	counters map[string]int
	items    []string
}

func NewComplexActor(id string) *ComplexActor {
	return &ComplexActor{
		id:       id,
		counters: make(map[string]int),
		items:    make([]string, 0),
	}
}

func (a *ComplexActor) IncrementCounter(name string, delta int) int {
	a.counters[name] += delta
	return a.counters[name]
}

func (a *ComplexActor) GetCounter(name string) int {
	return a.counters[name]
}

func (a *ComplexActor) AddItem(item string) int {
	a.items = append(a.items, item)
	return len(a.items)
}

func (a *ComplexActor) GetItems() []string {
	return a.items
}

func (a *ComplexActor) GetAllCounters() map[string]int {
	result := make(map[string]int)
	for k, v := range a.counters {
		result[k] = v
	}
	return result
}

func (a *ComplexActor) ProcessBatch(operations []map[string]interface{}) []interface{} {
	results := make([]interface{}, 0, len(operations))

	for _, op := range operations {
		opType := op["type"].(string)
		switch opType {
		case "increment":
			name := op["name"].(string)
			delta := int(op["delta"].(float64))
			result := a.IncrementCounter(name, delta)
			results = append(results, result)
		case "add_item":
			item := op["item"].(string)
			result := a.AddItem(item)
			results = append(results, result)
		case "get_counter":
			name := op["name"].(string)
			result := a.GetCounter(name)
			results = append(results, result)
		}
	}

	return results
}

// Actor that can be slow for testing timeouts/cancellation
type SlowActor struct {
	id int
}

func NewSlowActor(id int) *SlowActor {
	return &SlowActor{id: id}
}

func (a *SlowActor) SlowOperation(duration int, value string) string {
	time.Sleep(time.Duration(duration) * time.Millisecond)
	return fmt.Sprintf("slow_%d_%s", a.id, value)
}

func (a *SlowActor) VerySlowOperation(duration int) string {
	time.Sleep(time.Duration(duration) * time.Second)
	return fmt.Sprintf("very_slow_%d", a.id)
}

func (a *SlowActor) GetId() int {
	return a.id
}

// Actor with error scenarios
type ErrorActor struct {
	shouldFail bool
}

func NewErrorActor(shouldFail bool) *ErrorActor {
	return &ErrorActor{shouldFail: shouldFail}
}

func (a *ErrorActor) MightFail(message string) string {
	if a.shouldFail {
		panic("Actor intentionally failed: " + message)
	}
	return "success: " + message
}

func (a *ErrorActor) ToggleFailure() bool {
	a.shouldFail = !a.shouldFail
	return a.shouldFail
}

func (a *ErrorActor) DivideByZero(a1, b int) int {
	return a1 / b
}

// Resource intensive actor
type ResourceActor struct {
	data [][]int
}

func NewResourceActor(size int) *ResourceActor {
	data := make([][]int, size)
	for i := range data {
		data[i] = make([]int, size)
		for j := range data[i] {
			data[i][j] = i*size + j
		}
	}
	return &ResourceActor{data: data}
}

func (a *ResourceActor) ComputeSum() int {
	sum := 0
	for _, row := range a.data {
		for _, val := range row {
			sum += val
		}
	}
	return sum
}

func (a *ResourceActor) GetSize() int {
	return len(a.data)
}

func init() {
	// Register all actor types
	statefulActorName := RegisterActor(NewStatefulActor)
	complexActorName := RegisterActor(NewComplexActor)
	slowActorName := RegisterActor(NewSlowActor)
	errorActorName := RegisterActor(NewErrorActor)
	resourceActorName := RegisterActor(NewResourceActor)

	AddTestCase("TestBasicActorLifecycle", func(assert *require.Assertions) {
		// Test basic actor creation and method calls
		actor := ray.NewActor(statefulActorName, 10)

		// Test initial value
		ref1 := actor.RemoteCall("GetValue")
		value, err := ref1.Get1()
		assert.Nil(err)
		assert.Equal(10, value)

		// Test state modification
		ref2 := actor.RemoteCall("Add", 5)
		newValue, err := ref2.Get1()
		assert.Nil(err)
		assert.Equal(15, newValue)

		// Verify state persisted
		ref3 := actor.RemoteCall("GetValue")
		finalValue, err := ref3.Get1()
		assert.Nil(err)
		assert.Equal(15, finalValue)
	})

	AddTestCase("TestActorStateHistory", func(assert *require.Assertions) {
		actor := ray.NewActor(statefulActorName, 0)

		// Perform sequence of operations
		actor.RemoteCall("Add", 10).Get1()
		actor.RemoteCall("SetValue", 20).Get1()
		actor.RemoteCall("Add", 5).Get1()

		// Check history
		ref := actor.RemoteCall("GetHistory")
		history, err := ref.Get1()
		assert.Nil(err)

		historySlice := history.([]int)
		assert.Equal(historySlice, []int{10, 20, 25}) // History should contain all values
	})

	AddTestCase("TestComplexActorState", func(assert *require.Assertions) {
		actor := ray.NewActor(complexActorName, "test_complex")

		// Test counter operations
		ref1 := actor.RemoteCall("IncrementCounter", "counter1", 5)
		result1, err := ref1.Get1()
		assert.Nil(err)
		assert.Equal(5, result1)

		ref2 := actor.RemoteCall("IncrementCounter", "counter2", 10)
		result2, err := ref2.Get1()
		assert.Nil(err)
		assert.Equal(10, result2)

		// Test item operations
		ref3 := actor.RemoteCall("AddItem", "item1")
		count1, err := ref3.Get1()
		assert.Nil(err)
		assert.Equal(1, count1)

		ref4 := actor.RemoteCall("AddItem", "item2")
		count2, err := ref4.Get1()
		assert.Nil(err)
		assert.Equal(2, count2)

		// Test getting all state
		ref5 := actor.RemoteCall("GetAllCounters")
		counters, err := ref5.Get1()
		assert.Nil(err)

		counterMap := counters.(map[string]int)
		assert.Equal(5, counterMap["counter1"])
		assert.Equal(10, counterMap["counter2"])

		ref6 := actor.RemoteCall("GetItems")
		items, err := ref6.Get1()
		assert.Nil(err)

		itemSlice := items.([]string)
		assert.Equal([]string{"item1", "item2"}, itemSlice)
	})

	AddTestCase("TestActorBatchOperations", func(assert *require.Assertions) {
		actor := ray.NewActor(complexActorName, "batch_test")

		operations := []map[string]interface{}{
			{"type": "increment", "name": "batch_counter", "delta": float64(5)},
			{"type": "add_item", "item": "batch_item1"},
			{"type": "increment", "name": "batch_counter", "delta": float64(3)},
			{"type": "add_item", "item": "batch_item2"},
			{"type": "get_counter", "name": "batch_counter"},
		}

		ref := actor.RemoteCall("ProcessBatch", operations)
		results, err := ref.Get1()
		assert.Nil(err)

		resultSlice := results.([]interface{})
		assert.Len(resultSlice, 5)
		assert.Equal(5, resultSlice[0]) // First increment result
		assert.Equal(1, resultSlice[1]) // First item count
		assert.Equal(8, resultSlice[2]) // Second increment result
		assert.Equal(2, resultSlice[3]) // Second item count
		assert.Equal(8, resultSlice[4]) // Final counter value
	})

	AddTestCase("TestMultipleActorInstances", func(assert *require.Assertions) {
		// Create multiple instances of the same actor type
		actor1 := ray.NewActor(statefulActorName, 100)
		actor2 := ray.NewActor(statefulActorName, 200)
		actor3 := ray.NewActor(statefulActorName, 300)

		// Verify they have independent state
		ref1 := actor1.RemoteCall("GetValue")
		ref2 := actor2.RemoteCall("GetValue")
		ref3 := actor3.RemoteCall("GetValue")

		val1, _ := ref1.Get1()
		val2, _ := ref2.Get1()
		val3, _ := ref3.Get1()

		assert.Equal(100, val1)
		assert.Equal(200, val2)
		assert.Equal(300, val3)

		// Modify one actor and verify others unchanged
		actor2.RemoteCall("Add", 50).Get1()

		ref1b := actor1.RemoteCall("GetValue")
		ref2b := actor2.RemoteCall("GetValue")
		ref3b := actor3.RemoteCall("GetValue")

		val1b, _ := ref1b.Get1()
		val2b, _ := ref2b.Get1()
		val3b, _ := ref3b.Get1()

		assert.Equal(100, val1b) // Unchanged
		assert.Equal(250, val2b) // Changed
		assert.Equal(300, val3b) // Unchanged
	})

	AddTestCase("TestNamedActors", func(assert *require.Assertions) {
		// Create named actor
		actorName := "named_stateful_actor"
		actor1 := ray.NewActor(statefulActorName, 42, ray.Option("name", actorName))

		// Set some state
		actor1.RemoteCall("SetValue", 100).Get1()

		// Get the same actor by name
		actor2, err := ray.GetActor(actorName)
		assert.Nil(err)

		// Verify it's the same actor (same state)
		ref := actor2.RemoteCall("GetValue")
		value, err := ref.Get1()
		assert.Nil(err)
		assert.Equal(100, value)
	})

	AddTestCase("TestActorConcurrency", func(assert *require.Assertions) {
		actor := ray.NewActor(statefulActorName, 0)

		// Launch multiple concurrent operations
		var refs []ray.ObjectRef
		for i := 1; i <= 10; i++ {
			ref := actor.RemoteCall("Add", i)
			refs = append(refs, ref)
		}

		// Wait for all to complete
		var results []int
		for _, ref := range refs {
			result, err := ref.Get1()
			assert.Nil(err)
			results = append(results, result.(int))
		}

		// Final value should be sum of 1+2+...+10 = 55
		finalRef := actor.RemoteCall("GetValue")
		finalValue, err := finalRef.Get1()
		assert.Nil(err)
		assert.Equal(55, finalValue)

		// All intermediate results should be present
		assert.Len(results, 10)
	})

	AddTestCase("TestActorTimeout", func(assert *require.Assertions) {
		actor := ray.NewActor(slowActorName, 1)

		// Start a slow operation
		ref := actor.RemoteCall("VerySlowOperation", 2) // 2 seconds

		// Try to get result with timeout
		_, err := ref.GetAllTimeout(0.5) // 500ms timeout
		assert.ErrorIs(err, ray.ErrTimeout)
	})

	AddTestCase("TestActorCancellation", func(assert *require.Assertions) {
		actor := ray.NewActor(slowActorName, 2)

		// Start slow operation
		ref := actor.RemoteCall("VerySlowOperation", 5) // 5 seconds

		// Cancel it
		err := ref.Cancel()
		assert.Nil(err)

		// Getting result should fail
		_, err2 := ref.GetAll()
		assert.ErrorIs(err2, ray.ErrCancelled)
	})

	AddTestCase("TestActorErrorHandling", func(assert *require.Assertions) {
		actor := ray.NewActor(errorActorName, true) // Will fail

		// Call method that will panic
		ref := actor.RemoteCall("MightFail", "test_error")
		_, err := ref.Get1()
		assert.NotNil(err) // Should have error

		// Toggle failure mode
		toggleRef := actor.RemoteCall("ToggleFailure")
		newMode, err := toggleRef.Get1()
		assert.Nil(err)
		assert.Equal(false, newMode) // Should now be false

		// Now call should succeed
		ref2 := actor.RemoteCall("MightFail", "test_success")
		result, err := ref2.Get1()
		assert.Nil(err)
		assert.Equal("success: test_success", result)
	})

	AddTestCase("TestActorKill", func(assert *require.Assertions) {
		actor := ray.NewActor(slowActorName, 3)

		// Start a long-running operation
		ref := actor.RemoteCall("VerySlowOperation", 10) // 10 seconds

		// Kill the actor
		err := actor.Kill()
		assert.Nil(err)

		// The operation should fail
		_, err2 := ref.GetAll()
		assert.NotNil(err2)
	})

	AddTestCase("TestResourceIntensiveActor", func(assert *require.Assertions) {
		// Create actor with resource constraints
		actor := ray.NewActor(resourceActorName, 100,
			ray.Option("num_cpus", 1),
			ray.Option("memory", 100*1024*1024)) // 100MB

		ref1 := actor.RemoteCall("GetSize")
		size, err := ref1.Get1()
		assert.Nil(err)
		assert.Equal(100, size)

		ref2 := actor.RemoteCall("ComputeSum")
		sum, err := ref2.Get1()
		assert.Nil(err)
		// Sum should be 0+1+2+...+9999 = 9999*10000/2 = 49995000
		assert.Equal(49995000, sum)
	})

	AddTestCase("TestActorWithObjectRefs", func(assert *require.Assertions) {
		actor := ray.NewActor(complexActorName, "objref_test")

		// Create ObjectRef and pass to actor
		items := []string{"ref_item1", "ref_item2", "ref_item3"}
		_, err := ray.Put(items)
		assert.Nil(err)

		// This would require modifying the actor to accept ObjectRefs
		// For now, just test that we can pass regular data
		for _, item := range items {
			actor.RemoteCall("AddItem", item).Get1()
		}

		ref := actor.RemoteCall("GetItems")
		result, err := ref.Get1()
		assert.Nil(err)
		assert.Equal(items, result)
	})
}
