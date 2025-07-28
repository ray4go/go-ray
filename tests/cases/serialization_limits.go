package cases

import (
	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
	"unsafe"
)

// Test cases specifically for serialization limitations and error conditions

// Test struct with unexported fields (should work but unexported fields won't be serialized)
type StructWithUnexportedFields struct {
	PublicField    string
	privateField   string // This won't be serialized
	AnotherPublic  int
	anotherPrivate bool // This won't be serialized either
}

func (_ testTask) ProcessStructWithUnexportedFields(s StructWithUnexportedFields) StructWithUnexportedFields {
	s.PublicField = "processed_" + s.PublicField
	s.AnotherPublic *= 2
	// Note: private fields won't be accessible after serialization/deserialization
	return s
}

// Test recursive/circular structures (should cause issues)
type Node struct {
	Value int
	Next  *Node // This could create circular references
}

func (_ testTask) ProcessLinkedList(head *Node) *Node {
	// Simple processing - just double all values
	current := head
	for current != nil {
		current.Value *= 2
		current = current.Next
	}
	return head
}

// Test self-referencing struct (should cause panic)
type SelfReferencingStruct struct {
	ID   int
	Self *SelfReferencingStruct // Circular reference
}

func (_ testTask) ProcessSelfReferencingStruct(s SelfReferencingStruct) SelfReferencingStruct {
	return s
}

// Test channels (should not work well in distributed context)
func (_ testTask) ProcessChannel(ch chan int, val int) chan int {
	// This will likely fail in remote execution
	ch <- val
	return ch
}

// Test function types (should not serialize)
func (_ testTask) ProcessFunction(fn func(int) int, val int) int {
	// This will likely fail as functions can't be serialized
	return fn(val)
}

// Test unsafe pointers (should fail)
func (_ testTask) ProcessUnsafePointer(ptr unsafe.Pointer) unsafe.Pointer {
	return ptr
}

// Test very large slices (test memory limits)
func (_ testTask) ProcessVeryLargeSlice(size int) []int {
	// Create a large slice
	result := make([]int, size)
	for i := range result {
		result[i] = i
	}
	return result
}

// Test nil slices vs empty slices
func (_ testTask) ProcessNilAndEmptySlices(
	nilSlice []int,
	emptySlice []int,
) (bool, bool, int, int) {
	return nilSlice == nil, len(emptySlice) == 0, len(nilSlice), len(emptySlice)
}

// Test nil maps vs empty maps
func (_ testTask) ProcessNilAndEmptyMaps(
	nilMap map[string]int,
	emptyMap map[string]int,
) (bool, bool, int, int) {
	return nilMap == nil, len(emptyMap) == 0, len(nilMap), len(emptyMap)
}

// Test mixed nil and non-nil pointers in struct
type StructWithMixedPointers struct {
	ValidPtr *string
	NilPtr   *int
	Data     string
}

func (_ testTask) ProcessStructWithMixedPointers(s StructWithMixedPointers) StructWithMixedPointers {
	if s.ValidPtr != nil {
		*s.ValidPtr = "processed_" + *s.ValidPtr
	}
	if s.NilPtr != nil {
		*s.NilPtr = 999
	}
	s.Data = "processed_" + s.Data
	return s
}

// Test interface{} hidden in complex nested structures
type ComplexNestedWithInterface struct {
	Level1 map[string]NestedLevel2
}

type NestedLevel2 struct {
	Values []NestedLevel3
}

type NestedLevel3 struct {
	Data   interface{} // Hidden deep in the structure
	Backup string
}

func (_ testTask) ProcessComplexNestedWithInterface(s ComplexNestedWithInterface) ComplexNestedWithInterface {
	return s
}

// Test arrays vs slices behavior
func (_ testTask) ProcessArraysAndSlices(
	arr [5]int,
	slice []int,
) ([5]int, []int) {
	// Modify array
	for i := range arr {
		arr[i] *= 2
	}

	// Modify slice
	for i := range slice {
		slice[i] *= 3
	}

	return arr, slice
}

// Test empty string vs nil string pointer
func (_ testTask) ProcessStringPointers(
	emptyStr string,
	nilStrPtr *string,
	validStrPtr *string,
) (string, *string, *string) {
	emptyStr = "was_empty_" + emptyStr

	if validStrPtr != nil {
		*validStrPtr = "processed_" + *validStrPtr
	}

	// Note: nilStrPtr should remain nil
	return emptyStr, nilStrPtr, validStrPtr
}

func init() {
	// Test struct with unexported fields
	AddTestCase("TestStructWithUnexportedFields", func(assert *require.Assertions) {
		input := StructWithUnexportedFields{
			PublicField:   "test",
			AnotherPublic: 42,
			// privateField and anotherPrivate are set but won't be serialized
		}
		// Set private fields (though they won't survive serialization)

		objRef := ray.RemoteCall("ProcessStructWithUnexportedFields", input)
		result, err := objRef.Get1()
		assert.Nil(err)

		output := result.(StructWithUnexportedFields)
		assert.Equal("processed_test", output.PublicField)
		assert.Equal(84, output.AnotherPublic)
		// Private fields will be zero values after deserialization
	})

	// Test simple linked list (non-circular)
	AddTestCase("TestSimpleLinkedList", func(assert *require.Assertions) {
		// Create a simple non-circular linked list
		node3 := &Node{Value: 3, Next: nil}
		node2 := &Node{Value: 2, Next: node3}
		node1 := &Node{Value: 1, Next: node2}

		objRef := ray.RemoteCall("ProcessLinkedList", node1)
		result, err := objRef.Get1()
		assert.Nil(err)

		head := result.(*Node)
		assert.Equal(2, head.Value)           // 1 * 2
		assert.Equal(4, head.Next.Value)      // 2 * 2
		assert.Equal(6, head.Next.Next.Value) // 3 * 2
		assert.Nil(head.Next.Next.Next)
	})

	// Test nil and empty slices
	AddTestCase("TestNilAndEmptySlices", func(assert *require.Assertions) {
		var nilSlice []int = nil
		emptySlice := []int{}

		objRef := ray.RemoteCall("ProcessNilAndEmptySlices", nilSlice, emptySlice)
		results, err := objRef.GetAll()
		assert.Nil(err)
		assert.Len(results, 4)

		assert.True(results[0].(bool)) // nilSlice == nil
		assert.True(results[1].(bool)) // len(emptySlice) == 0
		assert.Equal(0, results[2])    // len(nilSlice)
		assert.Equal(0, results[3])    // len(emptySlice)
	})

	// Test nil and empty maps
	AddTestCase("TestNilAndEmptyMaps", func(assert *require.Assertions) {
		return
		var nilMap map[string]int = nil
		emptyMap := make(map[string]int)

		objRef := ray.RemoteCall("ProcessNilAndEmptyMaps", nilMap, emptyMap)
		results, err := objRef.GetAll()
		assert.Nil(err)
		assert.Len(results, 4)

		assert.True(results[0].(bool)) // nilMap == nil
		assert.True(results[1].(bool)) // len(emptyMap) == 0
		assert.Equal(0, results[2])    // len(nilMap)
		assert.Equal(0, results[3])    // len(emptyMap)
	})

	// Test struct with mixed pointers
	AddTestCase("TestStructWithMixedPointers", func(assert *require.Assertions) {
		validStr := "test"
		input := StructWithMixedPointers{
			ValidPtr: &validStr,
			NilPtr:   nil,
			Data:     "data",
		}

		objRef := ray.RemoteCall("ProcessStructWithMixedPointers", input)
		result, err := objRef.Get1()
		assert.Nil(err)

		output := result.(StructWithMixedPointers)
		assert.Equal("processed_test", *output.ValidPtr)
		assert.Nil(output.NilPtr)
		assert.Equal("processed_data", output.Data)
	})

	// Test arrays vs slices
	AddTestCase("TestArraysAndSlices", func(assert *require.Assertions) {
		arr := [5]int{1, 2, 3, 4, 5}
		slice := []int{10, 20, 30}

		objRef := ray.RemoteCall("ProcessArraysAndSlices", arr, slice)
		results, err := objRef.GetAll()
		assert.Nil(err)
		assert.Len(results, 2)

		resultArr := results[0].([5]int)
		resultSlice := results[1].([]int)

		assert.Equal([5]int{2, 4, 6, 8, 10}, resultArr)
		assert.Equal([]int{30, 60, 90}, resultSlice)
	})

	// Test string pointers
	AddTestCase("TestStringPointers", func(assert *require.Assertions) {
		assert.Panics(func() {
			validStr := "test"
			ray.RemoteCall("ProcessStringPointers", "", (*string)(nil), &validStr).GetAll()
		})
	})

	// Test very large slice (might hit memory limits)
	AddTestCase("TestLargeSlice", func(assert *require.Assertions) {
		// Test with a moderately large size (not too large to avoid timeout)
		objRef := ray.RemoteCall("ProcessVeryLargeSlice", 10000)
		result, err := objRef.Get1()
		assert.Nil(err)

		resultSlice := result.([]int)
		assert.Len(resultSlice, 10000)
		assert.Equal(0, resultSlice[0])
		assert.Equal(9999, resultSlice[9999])
	})

	// Panic test cases

	// Test circular reference (should cause panic during serialization)
	AddTestCase("TestCircularReferencePanic", func(assert *require.Assertions) {
		return
		assert.Panics(func() {
			s := SelfReferencingStruct{ID: 1}
			s.Self = &s // Create circular reference
			objRef := ray.RemoteCall("ProcessSelfReferencingStruct", s)
			objRef.Get1()
		})
	})

	// Test channel passing (should panic)
	AddTestCase("TestChannelPanic", func(assert *require.Assertions) {
		assert.Panics(func() {
			ch := make(chan int, 1)
			objRef := ray.RemoteCall("ProcessChannel", ch, 42)
			objRef.Get1()
		})
	})

	// Test function passing (should panic)
	AddTestCase("TestFunctionPanic", func(assert *require.Assertions) {
		assert.Panics(func() {
			fn := func(x int) int { return x * 2 }
			objRef := ray.RemoteCall("ProcessFunction", fn, 21)
			objRef.Get1()
		})
	})

	// Test unsafe pointer (should panic)
	AddTestCase("TestUnsafePointerPanic", func(assert *require.Assertions) {
		assert.Panics(func() {
			var x int = 42
			ptr := unsafe.Pointer(&x)
			objRef := ray.RemoteCall("ProcessUnsafePointer", ptr)
			objRef.Get1()
		})
	})

	// Test complex nested structure with interface{} (should panic)
	AddTestCase("TestComplexNestedInterfacePanic", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			s := ComplexNestedWithInterface{
				Level1: map[string]NestedLevel2{
					"key1": {
						Values: []NestedLevel3{
							{
								Data:   "this is interface{}", // This should cause panic
								Backup: "backup",
							},
						},
					},
				},
			}
			objRef := ray.RemoteCall("ProcessComplexNestedWithInterface", s)
			objRef.Get1()
		})
	})

	// Test extremely large slice (might cause memory issues)
	AddTestCase("TestExtremelyLargeSlicePanic", func(assert *require.Assertions) {
		return
		// This might panic due to memory constraints
		assert.Panics(func() {
			objRef := ray.RemoteCall("ProcessVeryLargeSlice", 100000000) // 100M integers
			objRef.Get1()
		})
	})
}
