package cases

import (
	"time"

	"github.com/ray4go/go-ray/ray"
	"github.com/ray4go/go-ray/ray/generic"
	"github.com/stretchr/testify/require"
)

func init() {
	AddTestCase("TestBasicActorLifecycleWrapper", func(assert *require.Assertions) {
		actor := NewStatefulActor(10).Remote(ray.Option("num_cpus", 0.01))

		value, err := StatefulActor_GetValue(actor).Remote().Get()
		assert.NoError(err)
		assert.Equal(10, value)

		newValue, err := StatefulActor_Add(actor, 5).Remote().Get()
		assert.NoError(err)
		assert.Equal(15, newValue)

		finalValue, err := StatefulActor_GetValue(actor).Remote().Get()
		assert.NoError(err)
		assert.Equal(15, finalValue)
	})

	AddTestCase("TestActorStateHistoryWrapper", func(assert *require.Assertions) {
		actor := NewStatefulActor(0).Remote(ray.Option("num_cpus", 0.01))

		_, err := StatefulActor_Add(actor, 10).Remote().Get()
		assert.NoError(err)
		_, err = StatefulActor_SetValue(actor, 20).Remote().Get()
		assert.NoError(err)
		_, err = StatefulActor_Add(actor, 5).Remote().Get()
		assert.NoError(err)

		history, err := StatefulActor_GetHistory(actor).Remote().Get()
		assert.NoError(err)
		assert.Equal([]int{10, 20, 25}, history)
	})

	AddTestCase("TestComplexActorStateWrapper", func(assert *require.Assertions) {
		actor := NewComplexActor("test_complex").Remote(ray.Option("num_cpus", 0.01))

		result1, err := ComplexActor_IncrementCounter(actor, "counter1", 5).Remote().Get()
		assert.NoError(err)
		assert.Equal(5, result1)

		result2, err := ComplexActor_IncrementCounter(actor, "counter2", 10).Remote().Get()
		assert.NoError(err)
		assert.Equal(10, result2)

		count1, err := ComplexActor_AddItem(actor, "item1").Remote().Get()
		assert.NoError(err)
		assert.Equal(1, count1)

		count2, err := ComplexActor_AddItem(actor, "item2").Remote().Get()
		assert.NoError(err)
		assert.Equal(2, count2)

		counters, err := ComplexActor_GetAllCounters(actor).Remote().Get()
		assert.NoError(err)
		assert.Equal(5, counters["counter1"])
		assert.Equal(10, counters["counter2"])

		items, err := ComplexActor_GetItems(actor).Remote().Get()
		assert.NoError(err)
		assert.Equal([]string{"item1", "item2"}, items)
	})

	AddTestCase("TestActorBatchOperationsWrapper", func(assert *require.Assertions) {
		actor := NewComplexActor("batch_test").Remote(ray.Option("num_cpus", 0.01))

		operations := []map[string]interface{}{
			{"type": "increment", "name": "batch_counter", "delta": float64(5)},
			{"type": "add_item", "item": "batch_item1"},
			{"type": "increment", "name": "batch_counter", "delta": float64(3)},
			{"type": "add_item", "item": "batch_item2"},
			{"type": "get_counter", "name": "batch_counter"},
		}

		results, err := ComplexActor_ProcessBatch(actor, operations).Remote().Get()
		assert.NoError(err)
		assert.Len(results, 5)
		assert.EqualValues(5, results[0])
		assert.EqualValues(1, results[1])
		assert.EqualValues(8, results[2])
		assert.EqualValues(2, results[3])
		assert.EqualValues(8, results[4])
	})

	AddTestCase("TestMultipleActorInstancesWrapper", func(assert *require.Assertions) {
		actor1 := NewStatefulActor(100).Remote(ray.Option("num_cpus", 0.01))
		actor2 := NewStatefulActor(200).Remote(ray.Option("num_cpus", 0.01))
		actor3 := NewStatefulActor(300).Remote(ray.Option("num_cpus", 0.01))

		val1, err := StatefulActor_GetValue(actor1).Remote().Get()
		assert.NoError(err)
		val2, err := StatefulActor_GetValue(actor2).Remote().Get()
		assert.NoError(err)
		val3, err := StatefulActor_GetValue(actor3).Remote().Get()
		assert.NoError(err)

		assert.Equal(100, val1)
		assert.Equal(200, val2)
		assert.Equal(300, val3)

		_, err = StatefulActor_Add(actor2, 50).Remote().Get()
		assert.NoError(err)

		val1b, err := StatefulActor_GetValue(actor1).Remote().Get()
		assert.NoError(err)
		val2b, err := StatefulActor_GetValue(actor2).Remote().Get()
		assert.NoError(err)
		val3b, err := StatefulActor_GetValue(actor3).Remote().Get()
		assert.NoError(err)

		assert.Equal(100, val1b)
		assert.Equal(250, val2b)
		assert.Equal(300, val3b)
	})

	AddTestCase("TestNamedActorsWrapper", func(assert *require.Assertions) {
		actorName := "named_stateful_actor_wrapper"
		actor := NewStatefulActor(42).Remote(
			ray.Option("name", actorName),
			ray.Option("num_cpus", 0.01),
		)

		_, err := StatefulActor_SetValue(actor, 100).Remote().Get()
		assert.NoError(err)

		typedActor, err := ray.GetTypedActor[ActorStatefulActor](actorName)
		assert.NoError(err)

		value, err := StatefulActor_GetValue(typedActor).Remote().Get()
		assert.NoError(err)
		assert.Equal(100, value)
	})

	AddTestCase("TestActorConcurrencyWrapper", func(assert *require.Assertions) {
		actor := NewStatefulActor(0).Remote(ray.Option("num_cpus", 0.01))

		futures := make([]*generic.Future1[int], 0, 10)
		for i := 1; i <= 10; i++ {
			futures = append(futures, StatefulActor_Add(actor, i).Remote())
		}

		var results []int
		for _, future := range futures {
			result, err := future.Get()
			assert.NoError(err)
			results = append(results, result)
		}

		finalValue, err := StatefulActor_GetValue(actor).Remote().Get()
		assert.NoError(err)
		assert.Equal(55, finalValue)
		assert.Len(results, 10)
	})

	AddTestCase("TestActorTimeoutWrapper", func(assert *require.Assertions) {
		actor := NewSlowActor(1).Remote(ray.Option("num_cpus", 0.01))

		future := SlowActor_VerySlowOperation(actor, 2).Remote()
		_, err := future.Get(ray.WithTimeout(500 * time.Millisecond))
		assert.ErrorIs(err, ray.ErrTimeout)
	})

	AddTestCase("TestActorCancellationWrapper", func(assert *require.Assertions) {
		actor := NewSlowActor(2).Remote(ray.Option("num_cpus", 0.01))

		future := SlowActor_VerySlowOperation(actor, 5).Remote()
		err := future.ObjectRef().Cancel()
		assert.NoError(err)

		_, err = future.Get()
		assert.ErrorIs(err, ray.ErrCancelled)
	})

	AddTestCase("TestActorErrorHandlingWrapper", func(assert *require.Assertions) {
		actor := NewErrorActor(true).Remote(ray.Option("num_cpus", 0.01))

		_, err := ErrorActor_MightFail(actor, "test_error").Remote().Get()
		assert.NotNil(err)

		newMode, err := ErrorActor_ToggleFailure(actor).Remote().Get()
		assert.NoError(err)
		assert.False(newMode)

		result, err := ErrorActor_MightFail(actor, "test_success").Remote().Get()
		assert.NoError(err)
		assert.Equal("success: test_success", result)
	})

	AddTestCase("TestActorKillWrapper", func(assert *require.Assertions) {
		actor := NewSlowActor(3).Remote(ray.Option("num_cpus", 0.01))

		future := SlowActor_VerySlowOperation(actor, 10).Remote()
		err := actor.Kill()
		assert.NoError(err)

		_, err = future.Get()
		assert.NotNil(err)
	})

	AddTestCase("TestResourceIntensiveActorWrapper", func(assert *require.Assertions) {
		actor := NewResourceActor(100).Remote(
			ray.Option("num_cpus", 1),
			ray.Option("memory", 100*1024*1024),
			ray.Option("num_cpus", 0.01),
		)

		size, err := ResourceActor_GetSize(actor).Remote().Get()
		assert.NoError(err)
		assert.Equal(100, size)

		sum, err := ResourceActor_ComputeSum(actor).Remote().Get()
		assert.NoError(err)
		assert.Equal(49995000, sum)
	})

	AddTestCase("TestActorWithObjectRefsWrapper", func(assert *require.Assertions) {
		actor := NewComplexActor("objref_test").Remote(ray.Option("num_cpus", 0.01))

		items := []string{"ref_item1", "ref_item2", "ref_item3"}
		_, err := ray.Put(items)
		assert.NoError(err)

		for _, item := range items {
			_, err := ComplexActor_AddItem(actor, item).Remote().Get()
			assert.NoError(err)
		}

		storedItems, err := ComplexActor_GetItems(actor).Remote().Get()
		assert.NoError(err)
		assert.Equal(items, storedItems)
	})

	AddTestCase("TestNewActorErrorWrapper", func(assert *require.Assertions) {
		actor := NewErrorTest().Remote(ray.Option("num_cpus", 0.01))
		_, err := ErrorTest_GetValue(actor).Remote().Get()
		assert.Error(err)
	})

	AddTestCase("TestCustomTypesWrapper", func(assert *require.Assertions) {
		ci, cs, csl, err := ProcessCustomTypes(CustomInt(10), CustomString("test"), CustomSlice{1, 2, 3}).Remote().Get()
		assert.NoError(err)
		assert.Equal(CustomInt(20), ci)
		assert.Equal(CustomString("test_processed"), cs)
		assert.Equal(CustomSlice{1, 2, 3, 999}, csl)
	})

	AddTestCase("TestTimeTypesWrapper", func(assert *require.Assertions) {
		now := time.Now()
		duration := 5 * time.Minute

		resultTime, resultDuration, err := ProcessTimeAndDuration(now, duration).Remote().Get()
		assert.NoError(err)
		assert.True(resultTime.After(now))
		assert.Equal(duration*2, resultDuration)
	})

	AddTestCase("TestNestedStructsWrapper", func(assert *require.Assertions) {
		input := OuterStruct{
			ID: 100,
			Inner: InnerStruct{
				Value: "inner",
				Count: 5,
			},
			List: []InnerStruct{
				{Value: "first", Count: 1},
				{Value: "second", Count: 2},
			},
		}

		output, err := ProcessNestedStructs(input).Remote().Get()
		assert.NoError(err)
		assert.Equal(200, output.ID)
		assert.Equal("processed_inner", output.Inner.Value)
		assert.Equal(15, output.Inner.Count)
		assert.Equal("batch_first", output.List[0].Value)
		assert.Equal("batch_second", output.List[1].Value)
		assert.Equal(1, output.List[0].Count)
		assert.Equal(3, output.List[1].Count)
	})

	AddTestCase("TestNestedCollectionsWrapper", func(assert *require.Assertions) {
		nestedMap := map[string]map[string]int{
			"outer1": {"inner1": 1, "inner2": 2},
			"outer2": {"inner3": 3, "inner4": 4},
		}
		nestedSlice := [][]string{
			{"a", "b"},
			{"c", "d", "e"},
		}
		mixedStructure := map[string][]int{
			"key1": {1, 2, 3},
			"key2": {4, 5},
		}

		processedMap, processedSlice, processedMixed, err := ProcessNestedCollections(nestedMap, nestedSlice, mixedStructure).Remote().Get()
		assert.NoError(err)
		assert.Equal(2, processedMap["outer1_outer"]["inner1_inner"])
		assert.Equal("a_processed", processedSlice[0][0])
		assert.Equal("e_processed", processedSlice[1][2])
		assert.Equal([]int{3, 6, 9}, processedMixed["key1_key"])
	})

	AddTestCase("TestVariadicWrapper", func(assert *require.Assertions) {
		resultSlice, err := ProcessVariadic(10, 1, 2, 3, 4, 5).Remote().Get()
		assert.NoError(err)
		assert.Equal([]int{11, 12, 13, 14, 15}, resultSlice)
	})

	AddTestCase("TestMultipleReturnTypesWrapper", func(assert *require.Assertions) {
		_, _, _, _, _, _, err := MultipleReturnTypes().Remote().Get()
		assert.NoError(err)
	})

	AddTestCase("TestLargeStructWrapper", func(assert *require.Assertions) {
		var large LargeStruct
		for i := 0; i < len(large.Data); i++ {
			large.Data[i] = i
		}
		large.Maps = map[int]string{1: "initial"}

		output, err := ProcessLargeStruct(large).Remote().Get()
		assert.NoError(err)
		assert.Equal(999, output.Data[0])
		assert.Equal(1000, output.Data[len(output.Data)-1])
		assert.Equal("processed", output.Maps[0])
		assert.Equal("initial", output.Maps[1])
	})

	AddTestCase("TestSliceOfInterfacePanicWrapper", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			data := []interface{}{"string", 42, true}
			_, err := TaskWithSliceOfInterface(data).Remote().Get()
			assert.NoError(err)
		})
	})

	AddTestCase("TestMapOfInterfacePanicWrapper", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			data := map[string]interface{}{
				"string": "value",
				"int":    42,
				"bool":   true,
			}
			_, err := TaskWithMapOfInterface(data).Remote().Get()
			assert.NoError(err)
		})
	})

	AddTestCase("TestDeeplyNestedInterfacePanicWrapper", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			s := DeeplyNestedStruct{
				Level1: Level1Struct{
					Level2: Level2Struct{Data: "hidden interface{} usage"},
				},
			}
			_, err := TaskWithDeeplyNestedInterface(s).Remote().Get()
			assert.NoError(err)
		})
	})

	AddTestCase("TestCallPythonCodeInTaskWrapper", func(assert *require.Assertions) {
		result, err := GetPythonEnvironment().Remote().Get()
		assert.NoError(err)
		assert.Contains(result, "Python")
	})

	AddTestCase("TestAdvancedWaitScenariosWrapper", func(assert *require.Assertions) {
		futures := []*generic.Future1[int]{
			VariableDelayTask(1, 50).Remote(),
			VariableDelayTask(2, 100).Remote(),
			VariableDelayTask(3, 200).Remote(),
			VariableDelayTask(4, 300).Remote(),
		}

		refs := make([]*ray.ObjectRef, len(futures))
		for i, future := range futures {
			refs[i] = future.ObjectRef()
		}

		ready, notReady, err := ray.Wait(refs, 2, ray.Option("timeout", 1.0))
		assert.NoError(err)
		assert.Len(ready, 2)
		assert.Len(notReady, 2)

		allReady, allNotReady, err := ray.Wait(append(ready, notReady...), 4, ray.Option("timeout", 2.0))
		assert.NoError(err)
		assert.Len(allReady, 4)
		assert.Len(allNotReady, 0)
	})

	AddTestCase("TestWaitWithFailuresWrapper", func(assert *require.Assertions) {
		futures := []*generic.Future1[int]{
			RandomFailureTask(1, false).Remote(),
			RandomFailureTask(2, true).Remote(),
			RandomFailureTask(3, false).Remote(),
			RandomFailureTask(4, true).Remote(),
		}

		refs := make([]*ray.ObjectRef, len(futures))
		for i, future := range futures {
			refs[i] = future.ObjectRef()
		}

		ready, notReady, err := ray.Wait(refs, len(refs), ray.Option("timeout", 2.0))
		assert.NoError(err)
		assert.Len(ready, len(refs))
		assert.Len(notReady, 0)

		successCount := 0
		failureCount := 0
		for _, ref := range ready {
			_, err := ray.Get1[int](ref)
			if err != nil {
				failureCount++
			} else {
				successCount++
			}
		}

		assert.Equal(2, successCount)
		assert.Equal(2, failureCount)
	})

	AddTestCase("TestComplexObjectRefChainingWrapper", func(assert *require.Assertions) {
		complexFuture := ReturnComplexObjectRef().Remote()
		result, err := ProcessComplexObjectRef(complexFuture).Remote().Get()
		assert.NoError(err)
		assert.Equal("Processed object 12345: complex_object", result)
	})

	AddTestCase("TestMixedTypeProcessingWrapper", func(assert *require.Assertions) {
		testMap := map[string]int{"a": 1, "b": 2, "c": 3}
		testSlice := []string{"item1", "item2", "item3"}

		resultMap, err := ProcessMixedTypes(42, 3.14, "test", true, testSlice, testMap).Remote().Get()
		assert.NoError(err)
		assert.EqualValues(84, resultMap["int_doubled"])
		assert.InDelta(9.8596, resultMap["float_squared"], 0.0001)
		assert.Equal("PROCESSED_test", resultMap["string_upper"])
		assert.Equal(false, resultMap["bool_negated"])
		assert.EqualValues(3, resultMap["slice_length"])
		assert.EqualValues(6, resultMap["map_sum"])
	})

	AddTestCase("TestTaskWithAllOptionTypesWrapper", func(assert *require.Assertions) {
		result, err := TaskWithAllOptions(10).Remote(
			ray.Option("num_cpus", 1),
			ray.Option("memory", 100*1024*1024),
			ray.Option("max_retries", 2),
			ray.Option("retry_exceptions", true),
		).Get()
		assert.NoError(err)
		assert.Equal(30, result)
	})

	AddTestCase("TestActorWithAdvancedOptionsWrapper", func(assert *require.Assertions) {
		actor := NewAdvancedOptionsActor("advanced_test").Remote(
			ray.Option("num_cpus", 0.1),
			ray.Option("memory", 200*1024*1024),
			ray.Option("max_restarts", 2),
			ray.Option("num_cpus", 0.01),
		)

		status, err := AdvancedOptionsActor_GetStatus(actor).Remote().Get()
		assert.NoError(err)
		assert.Equal("advanced_test", status["id"])
		assert.Equal(int64(1), status["call_count"])
		assert.Greater(status["uptime"].(float64), 0.0)
	})

	AddTestCase("TestActorMemoryIntensiveWithOptionsWrapper", func(assert *require.Assertions) {
		actor := NewAdvancedOptionsActor("memory_test").Remote(
			ray.Option("num_cpus", 2),
			ray.Option("memory", 500*1024*1024),
			ray.Option("num_cpus", 0.01),
		)

		result, err := AdvancedOptionsActor_MemoryIntensiveOperation(actor, 50).Remote().Get()
		assert.NoError(err)
		assert.Greater(result, 0)
	})

	AddTestCase("TestObjectRefTimeoutHandlingWrapper", func(assert *require.Assertions) {
		future := LongRunningTask(2, "timeout_test").Remote()

		_, err := future.Get(ray.WithTimeout(500 * time.Millisecond))
		assert.ErrorIs(err, ray.ErrTimeout)

		result, err := future.Get(ray.WithTimeout(3 * time.Second))
		assert.NoError(err)
		assert.Equal("long_task_timeout_test_completed", result)
	})

	AddTestCase("TestMultipleGetCallsOnSameObjectRefWrapper", func(assert *require.Assertions) {
		future := ProcessMixedTypes(5, 2.5, "multi", false, []string{"a"}, map[string]int{"x": 1}).Remote()
		future.ObjectRef().DisableAutoRelease()

		first, err := future.Get()
		assert.NoError(err)

		second, err := future.Get()
		assert.NoError(err)

		assert.Equal(first, second)
	})

	AddTestCase("TestObjectRefCancelAfterSuccessWrapper", func(assert *require.Assertions) {
		future := QuickTask(100).Remote()
		result, err := future.Get()
		assert.NoError(err)
		assert.Equal(200, result)

		cancelErr := future.ObjectRef().Cancel()
		_ = cancelErr
	})

	AddTestCase("TestLargeObjectRefBatchWrapper", func(assert *require.Assertions) {
		batchSize := 20
		futures := make([]*generic.Future1[int], 0, batchSize)
		for i := 0; i < batchSize; i++ {
			futures = append(futures, QuickTask(i).Remote())
		}

		refs := make([]*ray.ObjectRef, 0, batchSize)
		for _, future := range futures {
			refs = append(refs, future.ObjectRef())
		}

		ready, notReady, err := ray.Wait(refs, batchSize, ray.Option("timeout", 5.0))
		assert.NoError(err)
		assert.Len(ready, batchSize)
		assert.Len(notReady, 0)

		for _, ref := range ready {
			value, err := ray.Get1[int](ref)
			assert.NoError(err)
			assert.True(value >= 0 && value < batchSize*2)
		}
	})

	AddTestCase("TestEmptyAndNilInputsWrapper", func(assert *require.Assertions) {
		length, err := EmptySliceTask([]int{}).Remote().Get()
		assert.NoError(err)
		assert.Equal(0, length)

		nilMapResult, err := NilMapTask(map[string]int(nil)).Remote().Get()
		assert.NoError(err)
		assert.Equal(-1, nilMapResult)
	})

	AddTestCase("TestZeroValuesWrapper", func(assert *require.Assertions) {
		resultMap, err := ZeroValueTask("", 0, 0.0, false).Remote().Get()
		assert.NoError(err)

		assert.Equal(true, resultMap["string_empty"])
		assert.Equal(true, resultMap["int_zero"])
		assert.Equal(true, resultMap["float_zero"])
		assert.Equal(true, resultMap["bool_false"])
	})

	AddTestCase("TestLargeDataTransferWrapper", func(assert *require.Assertions) {
		resultStr, err := LargeStringTask(1024 * 100).Remote().Get()
		assert.NoError(err)
		assert.Equal(1024*100, len(resultStr))
		assert.Equal(byte('A'), resultStr[0])
		assert.Equal(byte('Z'), resultStr[25])
	})

	AddTestCase("TestNumericBoundariesWrapper", func(assert *require.Assertions) {
		resultMap, err := NumericBoundaryTask(2147483647, -2147483648, 1.7976931348623157e+308).Remote().Get()
		assert.NoError(err)
		assert.NotNil(resultMap["max_plus_one"])
		assert.NotNil(resultMap["min_minus_one"])
	})

	AddTestCase("TestConcurrentSlowTasksWrapper", func(assert *require.Assertions) {
		numTasks := 5
		futures := make([]*generic.Future1[int], 0, numTasks)
		for i := 0; i < numTasks; i++ {
			futures = append(futures, SlowIncrementerTask(i*10, 5, 20).Remote())
		}

		refs := make([]*ray.ObjectRef, len(futures))
		for i, future := range futures {
			refs[i] = future.ObjectRef()
		}

		ready, notReady, err := ray.Wait(refs, 3, ray.Option("timeout", 2.0))
		assert.NoError(err)
		assert.Len(ready, 3)
		assert.Len(notReady, 2)

		for _, ref := range ready {
			value, err := ray.Get1[int](ref)
			assert.NoError(err)
			assert.IsType(0, value)
		}

		for _, ref := range notReady {
			ref.Cancel()
		}
	})

	AddTestCase("TestMemoryLeakDetectionWrapper", func(assert *require.Assertions) {
		futures := make([]*generic.Future1[int], 0, 10)
		for i := 0; i < 10; i++ {
			futures = append(futures, MemoryLeakTest(100).Remote())
		}

		for _, future := range futures {
			total, err := future.Get()
			assert.NoError(err)
			assert.Equal(102400, total)
		}
	})

	AddTestCase("TestActorEdgeCasesWrapper", func(assert *require.Assertions) {
		actor := NewEdgeCaseActor().Remote(ray.Option("num_cpus", 0.01))
		assert.NoError(EdgeCaseActor_StoreNil(actor, "test_key").Remote().Get())

		sum, err := EdgeCaseActor_ProcessLargeArray(actor, 10000).Remote().Get()
		assert.NoError(err)
		assert.Equal(49995000, sum)
	})

	AddTestCase("TestActorInterruptionWrapper", func(assert *require.Assertions) {
		actor := NewEdgeCaseActor().Remote(ray.Option("num_cpus", 0.01))
		future := EdgeCaseActor_InterruptibleOperation(actor, 50).Remote()
		assert.NoError(future.ObjectRef().Cancel())

		_, err := future.Get()
		assert.ErrorIs(err, ray.ErrCancelled)
	})

	AddTestCase("TestActorErrorRecoveryWrapper", func(assert *require.Assertions) {
		actor := NewEdgeCaseActor().Remote(ray.Option("num_cpus", 0.01))

		result1, err := EdgeCaseActor_SimulateError(actor, false).Remote().Get()
		assert.NoError(err)
		assert.Equal("no_error", result1)

		_, err = EdgeCaseActor_SimulateError(actor, true).Remote().Get()
		assert.Error(err)

		history, err := EdgeCaseActor_GetCallHistory(actor).Remote().Get()
		assert.NoError(err)
		assert.Contains(history, "error_sim_false")
		assert.Contains(history, "error_sim_true")
	})

	AddTestCase("TestActorResourceCleanupWrapper", func(assert *require.Assertions) {
		actor := NewResourceCleanupActor().Remote(ray.Option("num_cpus", 0.01))
		_, err := ResourceCleanupActor_AllocateResource(actor, "resource1").Remote().Get()
		assert.NoError(err)
		_, err = ResourceCleanupActor_AllocateResource(actor, "resource2").Remote().Get()
		assert.NoError(err)
		_, err = ResourceCleanupActor_AllocateResource(actor, "resource3").Remote().Get()
		assert.NoError(err)

		count1, err := ResourceCleanupActor_GetResourceCount(actor).Remote().Get()
		assert.NoError(err)
		assert.Equal(3, count1)

		released, err := ResourceCleanupActor_ReleaseAllResources(actor).Remote().Get()
		assert.NoError(err)
		assert.Equal(3, released)

		count2, err := ResourceCleanupActor_GetResourceCount(actor).Remote().Get()
		assert.NoError(err)
		assert.Equal(0, count2)

		assert.NoError(actor.Kill())
	})

	AddTestCase("TestRapidActorCreationDestructionWrapper", func(assert *require.Assertions) {
		for i := 0; i < 10; i++ {
			actor := NewEdgeCaseActor().Remote(ray.Option("num_cpus", 0.01))
			_, err := EdgeCaseActor_GetCallHistory(actor).Remote().Get()
			assert.NoError(err)
			assert.NoError(actor.Kill())
		}
	})

	AddTestCase("TestObjectRefAfterTimeoutWrapper", func(assert *require.Assertions) {
		future := SlowIncrementerTask(0, 10, 100).Remote()
		_, err := future.Get(ray.WithTimeout(100 * time.Millisecond))
		assert.ErrorIs(err, ray.ErrTimeout)

		result, err := future.Get(ray.WithTimeout(2 * time.Second))
		assert.NoError(err)
		assert.Equal(10, result)
	})

	AddTestCase("TestMixedSuccessFailureBatchWrapper", func(assert *require.Assertions) {
		refs := []*ray.ObjectRef{
			QuickTask(1).Remote().ObjectRef(),
			DivideByZero(10, 0).Remote().ObjectRef(),
			QuickTask(2).Remote().ObjectRef(),
			TaskThatPanics("test").Remote().ObjectRef(),
			QuickTask(3).Remote().ObjectRef(),
		}

		ready, notReady, err := ray.Wait(refs, len(refs), ray.Option("timeout", 5.0))
		assert.NoError(err)
		assert.Len(ready, 5)
		assert.Len(notReady, 0)

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

	AddTestCase("TestPanicHandlingWrapper", func(assert *require.Assertions) {
		future := TaskThatPanics("test_panic").Remote()
		_, err := future.Get()
		assert.NotNil(err)
	})

	AddTestCase("TestDivisionByZeroWrapper", func(assert *require.Assertions) {
		future := DivideByZero(10, 0).Remote()
		_, err := future.Get()
		assert.NotNil(err)
	})

	AddTestCase("TestTimeoutBehaviorWrapper", func(assert *require.Assertions) {
		future := TaskWithDelay(500, "test").Remote()
		_, err := future.Get(ray.WithTimeout(100 * time.Millisecond))
		assert.ErrorIs(err, ray.ErrTimeout)
	})

	AddTestCase("TestCancelAfterCompletionWrapper", func(assert *require.Assertions) {
		future := TaskWithDelay(10, "fast").Remote()
		result, err := future.Get()
		assert.NoError(err)
		assert.Equal("delayed_fast", result)

		cancelErr := future.ObjectRef().Cancel()
		_ = cancelErr
	})

	AddTestCase("TestLargeDataProcessingWrapper", func(assert *require.Assertions) {
		size := 10000
		data, err := ProcessLargeData(size).Remote().Get()
		assert.NoError(err)
		assert.Len(data, size)
		assert.Equal(0, data[0])
		assert.Equal(1, data[1])
		assert.Equal(4, data[2])
		assert.Equal(9801, data[99])
	})

	AddTestCase("TestEmptyReturnValuesWrapper", func(assert *require.Assertions) {
		future := EmptyReturns().Remote()
		future.ObjectRef().DisableAutoRelease()

		assert.NoError(future.Get())

		results, err := future.ObjectRef().GetAll()
		assert.NoError(err)
		assert.Empty(results)
	})

	AddTestCase("TestMultipleReturnValuesWrapper", func(assert *require.Assertions) {
		future := MultipleReturnValues(15, 10).Remote()
		results, err := future.ObjectRef().GetAll()
		assert.NoError(err)
		assert.Len(results, 4)
		assert.Equal(25, results[0])
		assert.Equal(150, results[1])
		assert.Equal("15_10", results[2])
		assert.Equal(true, results[3])

		future2 := MultipleReturnValues(5, 8).Remote()
		val1, val2, val3, val4, err := future2.Get()
		assert.NoError(err)
		assert.Equal(13, val1)
		assert.Equal(40, val2)
		assert.Equal("5_8", val3)
		assert.Equal(false, val4)
	})

	AddTestCase("TestWaitWithTimeoutWrapper", func(assert *require.Assertions) {
		slowFuture := TaskWithDelay(1000, "slow").Remote()
		refs := []*ray.ObjectRef{slowFuture.ObjectRef()}

		ready, notReady, err := ray.Wait(refs, 1, ray.Option("timeout", 0.1))
		assert.NoError(err)
		assert.Empty(ready)
		assert.Len(notReady, 1)
		assert.Contains(notReady, slowFuture.ObjectRef())
	})

	AddTestCase("TestMultipleCancellationsWrapper", func(assert *require.Assertions) {
		future1 := TaskWithDelay(2000, "task1").Remote()
		future2 := TaskWithDelay(2000, "task2").Remote()
		future3 := TaskWithDelay(2000, "task3").Remote()

		assert.Nil(future1.ObjectRef().Cancel())
		assert.Nil(future2.ObjectRef().Cancel())
		assert.Nil(future3.ObjectRef().Cancel())

		_, err1 := future1.Get()
		_, err2 := future2.Get()
		_, err3 := future3.Get()

		assert.ErrorIs(err1, ray.ErrCancelled)
		assert.ErrorIs(err2, ray.ErrCancelled)
		assert.ErrorIs(err3, ray.ErrCancelled)
	})

	AddTestCase("TestPutBasicTypesWrapper", func(assert *require.Assertions) {
		intRef, err := ray.Put(42)
		assert.NoError(err)
		floatRef, err := ray.Put(3.14)
		assert.NoError(err)
		stringRef, err := ray.Put("hello")
		assert.NoError(err)
		boolRef, err := ray.Put(true)
		assert.NoError(err)

		val1, val2, val3, val4, err := ProcessPrimitiveTypes(intRef, floatRef, stringRef, boolRef).Remote().Get()
		assert.NoError(err)
		assert.Equal(84, val1)
		assert.Equal(6.28, val2)
		assert.Equal("hello_processed", val3)
		assert.False(val4)
	})

	AddTestCase("TestPutComplexStructWrapper", func(assert *require.Assertions) {
		testStruct := StorageTestStruct{
			ID:    100,
			Data:  []byte("test_data"),
			Value: 42.5,
		}

		objRef, err := ray.Put(testStruct)
		assert.NoError(err)

		result, err := ProcessStoredObject(objRef).Remote().Get()
		assert.NoError(err)

		expected := StorageTestStruct{
			ID:    200,
			Data:  []byte("test_data_processed"),
			Value: 63.75,
		}
		assert.Equal(expected, result)
	})

	AddTestCase("TestPutSliceWrapper", func(assert *require.Assertions) {
		slice := []int{1, 2, 3, 4, 5}
		sliceRef, err := ray.Put(slice)
		assert.NoError(err)

		result, err := ComputeSum(sliceRef).Remote().Get()
		assert.NoError(err)
		assert.Equal(15, result)
	})

	AddTestCase("TestPutMapWrapper", func(assert *require.Assertions) {
		testMap := map[string]int{"a": 1, "b": 2, "c": 3}
		mapRef, err := ray.Put(testMap)
		assert.NoError(err)

		result, err := ProcessMap(mapRef).Remote().Get()
		assert.NoError(err)
		assert.Equal(2, result["a_processed"])
		assert.Equal(4, result["b_processed"])
		assert.Equal(6, result["c_processed"])
	})

	AddTestCase("TestMultiplePutOperationsWrapper", func(assert *require.Assertions) {
		obj1 := StorageTestStruct{ID: 10, Data: []byte("first"), Value: 1.0}
		obj2 := StorageTestStruct{ID: 20, Data: []byte("second"), Value: 2.0}

		ref1, err := ray.Put(obj1)
		assert.NoError(err)
		ref2, err := ray.Put(obj2)
		assert.NoError(err)

		result, err := CombineStoredObjects(ref1, ref2).Remote().Get()
		assert.NoError(err)

		expected := StorageTestStruct{ID: 30, Data: []byte("firstsecond"), Value: 3.0}
		assert.Equal(expected, result)
	})

	AddTestCase("TestPutSliceOfStructsWrapper", func(assert *require.Assertions) {
		structs := []StorageTestStruct{
			{ID: 1, Data: []byte("one"), Value: 1.1},
			{ID: 2, Data: []byte("two"), Value: 2.2},
			{ID: 3, Data: []byte("three"), Value: 3.3},
		}

		structsRef, err := ray.Put(structs)
		assert.NoError(err)

		result, err := UseMultipleStoredObjects(structsRef).Remote().Get()
		assert.NoError(err)
		assert.Equal(6, result)
	})

	AddTestCase("TestPutLargeObjectWrapper", func(assert *require.Assertions) {
		largeData := make([]byte, 1024*10)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		largeStruct := StorageTestStruct{ID: 999, Data: largeData, Value: 123.456}
		objRef, err := ray.Put(largeStruct)
		assert.NoError(err)

		result, err := ProcessStoredObject(objRef).Remote().Get()
		assert.NoError(err)
		assert.Equal(1998, result.ID)
		assert.Equal(185.184, result.Value)
		assert.Len(result.Data, len(largeData)+len("_processed"))
	})

	AddTestCase("TestMixedPutAndRemoteCallWrapper", func(assert *require.Assertions) {
		value1, err := ray.Put(5)
		assert.NoError(err)
		ref1 := SlowAdd(value1, 10).Remote()

		value2, err := ray.Put(3)
		assert.NoError(err)

		result, err := SlowMultiply(ref1, value2).Remote().Get()
		assert.NoError(err)
		assert.Equal(45, result)
	})

	AddTestCase("TestObjReleaseWrapper", func(assert *require.Assertions) {
		data := StorageTestStruct{ID: 123, Data: []byte("to_be_released"), Value: 3.0}

		future := CombineStoredObjects(data, data).Remote()
		res, err := future.Get()
		assert.NoError(err)
		assert.Equal(246, res.ID)
		future.ObjectRef().Release()

		future2 := CombineStoredObjects(data, data).Remote()
		future2.ObjectRef().Release()
		_, err = future2.Get()
		assert.Error(err)
	})

}
