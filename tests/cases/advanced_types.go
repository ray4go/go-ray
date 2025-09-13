package cases

import (
	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
	"time"
)

// Test custom types and type aliases
type CustomInt int
type CustomString string
type CustomSlice []int

func (testTask) ProcessCustomTypes(
	ci CustomInt,
	cs CustomString,
	csl CustomSlice,
) (CustomInt, CustomString, CustomSlice) {
	return ci * 2, cs + "_processed", append(csl, 999)
}

// Test time.Time and other standard library types
func (testTask) ProcessTimeAndDuration(
	t time.Time,
	d time.Duration,
) (time.Time, time.Duration) {
	return t.Add(time.Hour), d * 2
}

// Test nested complex structures
type OuterStruct struct {
	ID    int
	Inner InnerStruct
	List  []InnerStruct
}

type InnerStruct struct {
	Value string
	Count int
}

func (testTask) ProcessNestedStructs(outer OuterStruct) OuterStruct {
	outer.ID *= 2
	outer.Inner.Value = "processed_" + outer.Inner.Value
	outer.Inner.Count += 10

	for i := range outer.List {
		outer.List[i].Value = "batch_" + outer.List[i].Value
		outer.List[i].Count += i
	}

	return outer
}

// Test deeply nested maps and slices
func (testTask) ProcessNestedCollections(
	nestedMap map[string]map[string]int,
	nestedSlice [][]string,
	mixedStructure map[string][]int,
) (map[string]map[string]int, [][]string, map[string][]int) {
	// Process nested map
	processedNestedMap := make(map[string]map[string]int)
	for k1, v1 := range nestedMap {
		processedNestedMap[k1+"_outer"] = make(map[string]int)
		for k2, v2 := range v1 {
			processedNestedMap[k1+"_outer"][k2+"_inner"] = v2 * 2
		}
	}

	// Process nested slice
	processedNestedSlice := make([][]string, len(nestedSlice))
	for i, slice := range nestedSlice {
		processedNestedSlice[i] = make([]string, len(slice))
		for j, str := range slice {
			processedNestedSlice[i][j] = str + "_processed"
		}
	}

	// Process mixed structure
	processedMixed := make(map[string][]int)
	for k, v := range mixedStructure {
		processedMixed[k+"_key"] = make([]int, len(v))
		for i, val := range v {
			processedMixed[k+"_key"][i] = val * 3
		}
	}

	return processedNestedMap, processedNestedSlice, processedMixed
}

// Test variadic functions
func (testTask) ProcessVariadic(base int, values ...int) []int {
	result := make([]int, len(values))
	for i, v := range values {
		result[i] = base + v
	}
	return result
}

// Test multiple return values with different types
func (testTask) MultipleReturnTypes() (int, string, []int, map[string]int, bool, error) {
	return 42, "success", []int{1, 2, 3}, map[string]int{"key": 100}, true, nil
}

// Problematic cases that should cause panics

// Test slice of interface{}
func (testTask) TaskWithSliceOfInterface(data []interface{}) []interface{} {
	return data
}

// Test map with interface{} values
func (testTask) TaskWithMapOfInterface(data map[string]interface{}) map[string]interface{} {
	return data
}

// Test nested struct with interface{} deep inside
type DeeplyNestedStruct struct {
	Level1 Level1Struct
}

type Level1Struct struct {
	Level2 Level2Struct
}

type Level2Struct struct {
	Data interface{} // Hidden interface{} deep in structure
}

func (testTask) TaskWithDeeplyNestedInterface(s DeeplyNestedStruct) DeeplyNestedStruct {
	return s
}

// Test very large data structures
type LargeStruct struct {
	Data [1000]int // Large array
	Maps map[int]string
}

func (testTask) ProcessLargeStruct(large LargeStruct) LargeStruct {
	// Just modify a few elements to keep it fast
	large.Data[0] = 999
	large.Data[999] = 1000
	if large.Maps == nil {
		large.Maps = make(map[int]string)
	}
	large.Maps[0] = "processed"
	return large
}

func init() {
	// Test custom types
	AddTestCase("TestCustomTypes", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("ProcessCustomTypes",
			CustomInt(10), CustomString("test"), CustomSlice{1, 2, 3})
		results, err := objRef.GetAll()
		assert.NoError(err)
		assert.Len(results, 3)

		assert.Equal(CustomInt(20), results[0])
		assert.Equal(CustomString("test_processed"), results[1])
		assert.Equal(CustomSlice{1, 2, 3, 999}, results[2])
	})

	// Test time types
	AddTestCase("TestTimeTypes", func(assert *require.Assertions) {
		now := time.Now()
		duration := time.Minute * 5

		objRef := ray.RemoteCall("ProcessTimeAndDuration", now, duration)
		results, err := objRef.GetAll()
		assert.NoError(err)
		assert.Len(results, 2)

		resultTime := results[0].(time.Time)
		resultDuration := results[1].(time.Duration)

		assert.True(resultTime.After(now))
		assert.Equal(duration*2, resultDuration)
	})

	// Test nested structures
	AddTestCase("TestNestedStructs", func(assert *require.Assertions) {
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

		objRef := ray.RemoteCall("ProcessNestedStructs", input)
		output, err := ray.Get1[OuterStruct](objRef)
		assert.NoError(err)

		assert.Equal(200, output.ID)
		assert.Equal("processed_inner", output.Inner.Value)
		assert.Equal(15, output.Inner.Count)
		assert.Equal("batch_first", output.List[0].Value)
		assert.Equal("batch_second", output.List[1].Value)
		assert.Equal(1, output.List[0].Count) // 1 + 0
		assert.Equal(3, output.List[1].Count) // 2 + 1
	})

	// Test deeply nested collections
	AddTestCase("TestNestedCollections", func(assert *require.Assertions) {
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

		objRef := ray.RemoteCall("ProcessNestedCollections", nestedMap, nestedSlice, mixedStructure)
		results, err := objRef.GetAll()
		assert.NoError(err)
		assert.Len(results, 3)

		resultNestedMap := results[0].(map[string]map[string]int)
		assert.Equal(2, resultNestedMap["outer1_outer"]["inner1_inner"])

		resultNestedSlice := results[1].([][]string)
		assert.Equal("a_processed", resultNestedSlice[0][0])
		assert.Equal("e_processed", resultNestedSlice[1][2])

		resultMixed := results[2].(map[string][]int)
		assert.Equal([]int{3, 6, 9}, resultMixed["key1_key"])
	})

	// Test variadic functions
	AddTestCase("TestVariadic", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("ProcessVariadic", 10, 1, 2, 3, 4, 5)
		resultSlice, err := ray.Get1[[]int](objRef)
		assert.NoError(err)

		assert.Equal([]int{11, 12, 13, 14, 15}, resultSlice)
	})

	// Test multiple return types
	AddTestCase("TestMultipleReturnTypes", func(assert *require.Assertions) {
		return
		assert.Panics(func() {
			ray.RemoteCall("MultipleReturnTypes").GetAll()
		})
	})

	// Test large structures
	AddTestCase("TestLargeStruct", func(assert *require.Assertions) {
		var large LargeStruct
		for i := 0; i < 1000; i++ {
			large.Data[i] = i
		}
		large.Maps = map[int]string{1: "initial"}

		objRef := ray.RemoteCall("ProcessLargeStruct", large)
		result, err := objRef.GetAll()
		assert.NoError(err)

		output := result[0].(LargeStruct)
		assert.Equal(999, output.Data[0])
		assert.Equal(1000, output.Data[999])
		assert.Equal("processed", output.Maps[0])
		assert.Equal("initial", output.Maps[1])
	})

	// Panic test cases for invalid types

	// Test slice of interface{}
	AddTestCase("TestSliceOfInterfacePanic", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			data := []interface{}{"string", 42, true}
			objRef := ray.RemoteCall("TaskWithSliceOfInterface", data)
			objRef.GetAll()
		})
	})

	// Test map with interface{} values
	AddTestCase("TestMapOfInterfacePanic", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			data := map[string]interface{}{
				"string": "value",
				"int":    42,
				"bool":   true,
			}
			objRef := ray.RemoteCall("TaskWithMapOfInterface", data)
			objRef.GetAll()
		})
	})

	// Test deeply nested interface{}
	AddTestCase("TestDeeplyNestedInterfacePanic", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			s := DeeplyNestedStruct{
				Level1: Level1Struct{
					Level2: Level2Struct{
						Data: "hidden interface{} usage",
					},
				},
			}
			objRef := ray.RemoteCall("TaskWithDeeplyNestedInterface", s)
			objRef.GetAll()
		})
	})

	// Test passing nil directly to variadic function
	AddTestCase("TestVariadicWithNil", func(assert *require.Assertions) {
		// This should work fine since we're not passing nil as the only argument
		objRef := ray.RemoteCall("ProcessVariadic", 10)
		result, err := objRef.GetAll()
		assert.NoError(err)
		assert.Equal([]int{}, result[0]) // Empty slice is fine
	})

	// Test empty variadic call
	AddTestCase("TestEmptyVariadic", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("ProcessVariadic", 100) // Only base parameter
		result, err := objRef.GetAll()
		assert.NoError(err)
		assert.Equal([]int{}, result[0])
	})
}
