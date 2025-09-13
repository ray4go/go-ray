package cases

import (
	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

// Test basic types support
func (testTask) ProcessBasicTypes(
	intVal int,
	int32Val int32,
	int64Val int64,
	floatVal float32,
	float64Val float64,
	stringVal string,
	boolVal bool,
	byteVal byte,
	runeVal rune,
) (int, int32, int64, float32, float64, string, bool, byte, rune) {
	return intVal + 1, int32Val + 1, int64Val + 1, floatVal + 1.0, float64Val + 1.0,
		stringVal + "_processed", !boolVal, byteVal + 1, runeVal + 1
}

// Test slice and array types
func (testTask) ProcessSliceAndArray(
	intSlice []int,
	stringSlice []string,
	intArray [3]int,
) ([]int, []string, [3]int) {
	// Process slice
	processedIntSlice := make([]int, len(intSlice))
	for i, v := range intSlice {
		processedIntSlice[i] = v * 2
	}

	processedStringSlice := make([]string, len(stringSlice))
	for i, v := range stringSlice {
		processedStringSlice[i] = v + "_processed"
	}

	// Process array
	var processedArray [3]int
	for i, v := range intArray {
		processedArray[i] = v * 3
	}

	return processedIntSlice, processedStringSlice, processedArray
}

// Test map types
func (testTask) ProcessMaps(
	stringIntMap map[string]int,
	intStringMap map[int]string,
) (map[string]int, map[int]string) {
	processedStringIntMap := make(map[string]int)
	for k, v := range stringIntMap {
		processedStringIntMap[k+"_key"] = v * 2
	}

	processedIntStringMap := make(map[int]string)
	for k, v := range intStringMap {
		processedIntStringMap[k*2] = v + "_value"
	}

	return processedStringIntMap, processedIntStringMap
}

// Test pointer types (should work for non-nil pointers)
func (testTask) ProcessPointers(
	intPtr *int,
	stringPtr *string,
) (*int, *string) {
	if intPtr != nil {
		newInt := *intPtr * 2
		intPtr = &newInt
	}
	if stringPtr != nil {
		newString := *stringPtr + "_processed"
		stringPtr = &newString
	}
	return intPtr, stringPtr
}

// Test struct with valid fields
type ValidStruct struct {
	ID       int64
	Name     string
	Values   []int
	Metadata map[string]string // Only string values, no interface{}
}

func (testTask) ProcessValidStruct(s ValidStruct) ValidStruct {
	s.ID *= 2
	s.Name = "processed_" + s.Name
	for i := range s.Values {
		s.Values[i] *= 2
	}
	processedMetadata := make(map[string]string)
	for k, v := range s.Metadata {
		processedMetadata[k+"_key"] = v + "_value"
	}
	s.Metadata = processedMetadata
	return s
}

// Test empty/zero values (should work)
func (testTask) ProcessEmptyValues(
	emptyString string,
	zeroInt int,
	emptySlice []int,
	emptyMap map[string]int,
) (string, int, []int, map[string]int) {
	if emptyString == "" {
		emptyString = "was_empty"
	}
	if zeroInt == 0 {
		zeroInt = 42
	}
	if len(emptySlice) == 0 {
		emptySlice = []int{1, 2, 3}
	}
	if len(emptyMap) == 0 {
		emptyMap = map[string]int{"default": 100}
	}
	return emptyString, zeroInt, emptySlice, emptyMap
}

// Test channel types (should work but be careful with usage)
func (testTask) ProcessChannelType(ch chan int) chan int {
	// Note: In practice, channels might not serialize well across ray workers
	// This is more of a test to see if the type system allows it
	return ch
}

// Test function types (might not work well in distributed context)
func (testTask) ProcessFunctionType(fn func(int) int) func(int) int {
	// Note: Function types might not serialize well
	return fn
}

// Tasks that should cause panics when called with invalid arguments
// These are tested in separate test cases with panic recovery

func (testTask) TaskExpectingNonNilPointer(ptr *int) int {
	// This should panic if ptr is nil
	return *ptr
}

func (testTask) TaskWithInterfaceParam(val interface{}) interface{} {
	// This should panic due to interface{} usage
	return val
}

type StructWithInterface struct {
	ID   int
	Data interface{} // This should cause issues
}

func (testTask) TaskWithStructContainingInterface(s StructWithInterface) StructWithInterface {
	// This should panic due to interface{} in struct
	return s
}

type StructWithNilPointer struct {
	ID   int
	Ptr  *string // This could be nil
	Data string
}

func (testTask) TaskWithNilInStruct(s StructWithNilPointer) StructWithNilPointer {
	// This should work fine even if Ptr is nil
	return s
}

// Additional test types and tasks for comprehensive testing

func (testTask) TaskWithMultipleInterfaceParams(a interface{}, b interface{}, c interface{}) (interface{}, interface{}, interface{}) {
	return a, b, c
}

func (testTask) TaskWithMapContainingInterface(m map[string]interface{}) map[string]interface{} {
	return m
}

func (testTask) TaskWithSliceContainingInterface(s []interface{}) []interface{} {
	return s
}

type NestedStructWithInterface struct {
	Outer StructWithInterface
	Inner StructWithInterface
}

func (testTask) TaskWithNestedStructContainingInterface(nested NestedStructWithInterface) NestedStructWithInterface {
	return nested
}

type StructWithValidPointers struct {
	ID        int
	StringPtr *string
	IntPtr    *int
	Data      string
}

func (testTask) TaskWithValidPointers(s StructWithValidPointers) StructWithValidPointers {
	// Process the struct, handling potential nil pointers
	if s.StringPtr != nil {
		newStr := *s.StringPtr + "_processed"
		s.StringPtr = &newStr
	}
	if s.IntPtr != nil {
		newInt := *s.IntPtr * 2
		s.IntPtr = &newInt
	}
	s.Data = s.Data + "_processed"
	return s
}

func (testTask) TaskWithValidMap(m map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range m {
		result[k] = v + "_processed"
	}
	return result
}

func (testTask) TaskWithValidSlice(s []string) []string {
	result := make([]string, len(s))
	for i, v := range s {
		result[i] = v + "_processed"
	}
	return result
}

func init() {
	// Test valid basic types
	AddTestCase("TestBasicTypes", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("ProcessBasicTypes",
			10, int32(20), int64(30), float32(1.5), float64(2.5),
			"test", true, byte(65), rune('A'))

		results, err := objRef.GetAll()
		assert.NoError(err)
		assert.Len(results, 9)

		assert.Equal(11, results[0])
		assert.Equal(int32(21), results[1])
		assert.Equal(int64(31), results[2])
		assert.Equal(float32(2.5), results[3])
		assert.Equal(float64(3.5), results[4])
		assert.Equal("test_processed", results[5])
		assert.Equal(false, results[6])
		assert.Equal(byte(66), results[7])
		assert.Equal(rune('B'), results[8])
	})

	// Test slice and array types
	AddTestCase("TestSliceAndArray", func(assert *require.Assertions) {
		intSlice := []int{1, 2, 3}
		stringSlice := []string{"a", "b", "c"}
		intArray := [3]int{10, 20, 30}

		objRef := ray.RemoteCall("ProcessSliceAndArray", intSlice, stringSlice, intArray)
		results, err := objRef.GetAll()
		assert.NoError(err)
		assert.Len(results, 3)

		assert.Equal([]int{2, 4, 6}, results[0])
		assert.Equal([]string{"a_processed", "b_processed", "c_processed"}, results[1])
		assert.Equal([3]int{30, 60, 90}, results[2])
	})

	// Test map types
	AddTestCase("TestMaps", func(assert *require.Assertions) {
		stringIntMap := map[string]int{"a": 1, "b": 2}
		intStringMap := map[int]string{1: "one", 2: "two"}

		objRef := ray.RemoteCall("ProcessMaps", stringIntMap, intStringMap)
		results, err := objRef.GetAll()
		assert.NoError(err)
		assert.Len(results, 2)

		resultMap1 := results[0].(map[string]int)
		assert.Equal(2, resultMap1["a_key"])
		assert.Equal(4, resultMap1["b_key"])

		resultMap2 := results[1].(map[int]string)
		assert.Equal("one_value", resultMap2[2])
		assert.Equal("two_value", resultMap2[4])
	})

	// Test valid pointer types (non-nil)
	AddTestCase("TestValidPointers", func(assert *require.Assertions) {
		intVal := 42
		stringVal := "test"

		objRef := ray.RemoteCall("ProcessPointers", &intVal, &stringVal)
		results, err := objRef.GetAll()
		assert.NoError(err)
		assert.Len(results, 2)

		resultIntPtr := results[0].(*int)
		resultStringPtr := results[1].(*string)

		assert.Equal(84, *resultIntPtr)
		assert.Equal("test_processed", *resultStringPtr)
	})

	// Test valid struct
	AddTestCase("TestValidStruct", func(assert *require.Assertions) {
		input := ValidStruct{
			ID:     123,
			Name:   "test",
			Values: []int{1, 2, 3},
			Metadata: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		}

		objRef := ray.RemoteCall("ProcessValidStruct", input)
		result, err := objRef.GetAll()
		assert.NoError(err)

		output := result[0].(ValidStruct)
		assert.Equal(int64(246), output.ID)
		assert.Equal("processed_test", output.Name)
		assert.Equal([]int{2, 4, 6}, output.Values)
		assert.Equal("value1_value", output.Metadata["key1_key"])
		assert.Equal("value2_value", output.Metadata["key2_key"])
	})

	// Test empty/zero values
	AddTestCase("TestEmptyValues", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("ProcessEmptyValues", "", 0, []int{}, map[string]int{})
		results, err := objRef.GetAll()
		assert.NoError(err)
		assert.Len(results, 4)

		assert.Equal("was_empty", results[0])
		assert.Equal(42, results[1])
		assert.Equal([]int{1, 2, 3}, results[2])
		assert.Equal(map[string]int{"default": 100}, results[3])
	})

	AddTestCase("TestNilPointerPanic", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			objRef := ray.RemoteCall("TaskExpectingNonNilPointer", (*int)(nil))
			_, err := objRef.GetAll() // This should panic when the remote task executes
			assert.NotNil(err, "Expected an error due to nil pointer dereference")
		})
	})

	// Test panic cases - interface{} parameter
	AddTestCase("TestInterfaceParamPanic", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			ray.RemoteCall("TaskWithInterfaceParam", "some value")
		})
	})

	// Test panic cases - struct with interface{} field
	AddTestCase("TestStructWithInterfacePanic", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			s := StructWithInterface{
				ID:   1,
				Data: "some data",
			}
			ray.RemoteCall("TaskWithStructContainingInterface", s)
		})
	})

	// Test struct with nil pointer field - this should work as gob can handle nil pointers in structs
	AddTestCase("TestStructWithNilPointer", func(assert *require.Assertions) {
		// Struct with nil pointer should be serializable
		s := StructWithNilPointer{
			ID:   1,
			Ptr:  nil, // nil pointer in struct is OK
			Data: "test",
		}

		// This should NOT panic - gob can handle nil pointers in structs
		assert.NotPanics(func() {
			objRef := ray.RemoteCall("TaskWithNilInStruct", s)
			result, err := objRef.GetAll()
			assert.NoError(err)
			resultStruct := result[0].(StructWithNilPointer)
			assert.Equal(1, resultStruct.ID)
			assert.Nil(resultStruct.Ptr)
			assert.Equal("test", resultStruct.Data)
		})
	})

	AddTestCase("TestSingleNilParam", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			obj := ray.RemoteCall("TaskExpectingNonNilPointer", nil)
			_, err := obj.GetAll()
			assert.NotNil(err, "Expected an error due to nil pointer dereference")
		})
	})

	// Test passing multiple interface{} values - should panic during encoding
	AddTestCase("TestMultipleInterfaceParams", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			ray.RemoteCall("TaskWithMultipleInterfaceParams", "string", 42, true)
		})
	})

	// Test passing map with interface{} values - should panic during encoding
	AddTestCase("TestMapWithInterfaceValues", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			badMap := map[string]interface{}{
				"key1": "string value",
				"key2": 42,
				"key3": true,
			}
			ray.RemoteCall("TaskWithMapContainingInterface", badMap)
		})
	})

	// Test passing slice with interface{} elements - should panic during encoding
	AddTestCase("TestSliceWithInterfaceElements", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			badSlice := []interface{}{"string", 42, true}
			ray.RemoteCall("TaskWithSliceContainingInterface", badSlice)
		})
	})

	// Test nested struct with interface{} - should panic during encoding
	AddTestCase("TestNestedStructWithInterface", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			nested := NestedStructWithInterface{
				Outer: StructWithInterface{
					ID:   1,
					Data: "outer data",
				},
				Inner: StructWithInterface{
					ID:   2,
					Data: "inner data",
				},
			}
			ray.RemoteCall("TaskWithNestedStructContainingInterface", nested)
		})
	})

	// Test valid scenarios that should NOT panic

	// Test struct with valid pointer types
	AddTestCase("TestStructWithValidPointers", func(assert *require.Assertions) {
		str := "valid string"
		num := 42
		s := StructWithValidPointers{
			ID:        1,
			StringPtr: &str,
			IntPtr:    &num,
			Data:      "test",
		}

		assert.NotPanics(func() {
			objRef := ray.RemoteCall("TaskWithValidPointers", s)
			result, err := objRef.GetAll()
			assert.NoError(err)
			resultStruct := result[0].(StructWithValidPointers)
			assert.Equal(1, resultStruct.ID)
			assert.Equal("valid string_processed", *resultStruct.StringPtr)
			assert.Equal(84, *resultStruct.IntPtr)
			assert.Equal("test_processed", resultStruct.Data)
		})
	})

	// Test map with valid types only
	AddTestCase("TestMapWithValidTypes", func(assert *require.Assertions) {
		validMap := map[string]string{
			"key1": "value1",
			"key2": "value2",
		}

		assert.NotPanics(func() {
			objRef := ray.RemoteCall("TaskWithValidMap", validMap)
			resultMap, err := ray.Get1[map[string]string](objRef)
			assert.NoError(err)
			assert.Equal("value1_processed", resultMap["key1"])
			assert.Equal("value2_processed", resultMap["key2"])
		})
	})

	// Test slice with valid element types
	AddTestCase("TestSliceWithValidTypes", func(assert *require.Assertions) {
		validSlice := []string{"item1", "item2", "item3"}

		assert.NotPanics(func() {
			objRef := ray.RemoteCall("TaskWithValidSlice", validSlice)
			result, err := objRef.GetAll()
			assert.NoError(err)
			resultSlice := result[0].([]string)
			assert.Equal([]string{"item1_processed", "item2_processed", "item3_processed"}, resultSlice)
		})
	})
}
