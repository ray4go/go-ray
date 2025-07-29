package cases

import (
	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

// Test ray.Put functionality and object store operations
type StorageTestStruct struct {
	ID    int
	Data  []byte
	Value float64
}

func (_ testTask) ProcessStoredObject(obj StorageTestStruct) StorageTestStruct {
	obj.ID *= 2
	obj.Value *= 1.5
	obj.Data = append(obj.Data, []byte("_processed")...)
	return obj
}

func (_ testTask) CombineStoredObjects(obj1, obj2 StorageTestStruct) StorageTestStruct {
	return StorageTestStruct{
		ID:    obj1.ID + obj2.ID,
		Data:  append(obj1.Data, obj2.Data...),
		Value: obj1.Value + obj2.Value,
	}
}

func (_ testTask) UseMultipleStoredObjects(objs []StorageTestStruct) int {
	total := 0
	for _, obj := range objs {
		total += obj.ID
	}
	return total
}

func (_ testTask) ProcessPrimitiveTypes(i int, f float64, s string, b bool) (int, float64, string, bool) {
	return i * 2, f * 2.0, s + "_processed", !b
}

func init() {
	AddTestCase("TestPutBasicTypes", func(assert *require.Assertions) {
		// Test putting primitive types
		intRef, err1 := ray.Put(42)
		floatRef, err2 := ray.Put(3.14)
		stringRef, err3 := ray.Put("hello")
		boolRef, err4 := ray.Put(true)

		assert.Nil(err1)
		assert.Nil(err2)
		assert.Nil(err3)
		assert.Nil(err4)

		// Use the stored objects in a task
		resultRef := ray.RemoteCall("ProcessPrimitiveTypes", intRef, floatRef, stringRef, boolRef)

		val1, val2, val3, val4, err := resultRef.Get4()
		assert.Nil(err)
		assert.Equal(84, val1)
		assert.Equal(6.28, val2)
		assert.Equal("hello_processed", val3)
		assert.Equal(false, val4)
	})

	AddTestCase("TestPutComplexStruct", func(assert *require.Assertions) {
		// Create and store a complex struct
		testStruct := StorageTestStruct{
			ID:    100,
			Data:  []byte("test_data"),
			Value: 42.5,
		}

		objRef, err := ray.Put(testStruct)
		assert.Nil(err)

		// Use the stored object in a task
		resultRef := ray.RemoteCall("ProcessStoredObject", objRef)
		result, err := resultRef.Get1()
		assert.Nil(err)

		expected := StorageTestStruct{
			ID:    200,
			Data:  []byte("test_data_processed"),
			Value: 63.75,
		}
		assert.Equal(expected, result)
	})

	AddTestCase("TestPutSlice", func(assert *require.Assertions) {
		// Store a slice
		slice := []int{1, 2, 3, 4, 5}
		sliceRef, err := ray.Put(slice)
		assert.Nil(err)

		// Use the stored slice in a task that expects []int
		// Note: We need a task that accepts []int
		// Let's create it inline for this test by using existing tasks
		resultRef := ray.RemoteCall("ComputeSum", sliceRef)
		result, err := resultRef.Get1()
		assert.Nil(err)
		assert.Equal(15, result) // 1+2+3+4+5 = 15
	})

	AddTestCase("TestPutMap", func(assert *require.Assertions) {
		// Store a map
		testMap := map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		}

		mapRef, err := ray.Put(testMap)
		assert.Nil(err)

		// Use the stored map
		resultRef := ray.RemoteCall("ProcessMap", mapRef)
		result, err := resultRef.Get1()
		assert.Nil(err)

		resultMap := result.(map[string]int)
		assert.Equal(2, resultMap["a_processed"])
		assert.Equal(4, resultMap["b_processed"])
		assert.Equal(6, resultMap["c_processed"])
	})

	AddTestCase("TestMultiplePutOperations", func(assert *require.Assertions) {
		// Store multiple objects
		obj1 := StorageTestStruct{
			ID:    10,
			Data:  []byte("first"),
			Value: 1.0,
		}
		obj2 := StorageTestStruct{
			ID:    20,
			Data:  []byte("second"),
			Value: 2.0,
		}

		ref1, err1 := ray.Put(obj1)
		ref2, err2 := ray.Put(obj2)
		assert.Nil(err1)
		assert.Nil(err2)

		// Combine the stored objects
		resultRef := ray.RemoteCall("CombineStoredObjects", ref1, ref2)
		result, err := resultRef.Get1()
		assert.Nil(err)

		expected := StorageTestStruct{
			ID:    30,
			Data:  []byte("firstsecond"),
			Value: 3.0,
		}
		assert.Equal(expected, result)
	})

	AddTestCase("TestPutSliceOfStructs", func(assert *require.Assertions) {
		// Store slice of structs
		structs := []StorageTestStruct{
			{ID: 1, Data: []byte("one"), Value: 1.1},
			{ID: 2, Data: []byte("two"), Value: 2.2},
			{ID: 3, Data: []byte("three"), Value: 3.3},
		}

		structsRef, err := ray.Put(structs)
		assert.Nil(err)

		// Process the stored slice
		resultRef := ray.RemoteCall("UseMultipleStoredObjects", structsRef)
		result, err := resultRef.Get1()
		assert.Nil(err)
		assert.Equal(6, result) // 1+2+3 = 6
	})

	AddTestCase("TestPutLargeObject", func(assert *require.Assertions) {
		// Create a larger object to test storage efficiency
		largeData := make([]byte, 1024*10) // 10KB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		largeStruct := StorageTestStruct{
			ID:    999,
			Data:  largeData,
			Value: 123.456,
		}

		objRef, err := ray.Put(largeStruct)
		assert.Nil(err)

		// Process the large object
		resultRef := ray.RemoteCall("ProcessStoredObject", objRef)
		result, err := resultRef.Get1()
		assert.Nil(err)

		resultStruct := result.(StorageTestStruct)
		assert.Equal(1998, resultStruct.ID)              // 999 * 2
		assert.Equal(185.184, resultStruct.Value)        // 123.456 * 1.5
		assert.Len(resultStruct.Data, len(largeData)+10) // original + "_processed"
	})

	AddTestCase("TestMixedPutAndRemoteCall", func(assert *require.Assertions) {
		// Mix Put operations with RemoteCall operations
		value1, err1 := ray.Put(5)
		assert.Nil(err1)

		// Use stored value in remote call
		ref1 := ray.RemoteCall("SlowAdd", value1, 10) // = 15

		// Store the result of remote call
		value2, err2 := ray.Put(3)
		assert.Nil(err2)

		// Chain operations
		ref2 := ray.RemoteCall("SlowMultiply", ref1, value2) // 15 * 3 = 45

		result, err := ref2.Get1()
		assert.Nil(err)
		assert.Equal(45, result)
	})

	AddTestCase("TestPutNilHandling", func(assert *require.Assertions) {
		assert.NotPanics(func() {
			var nilPtr *StorageTestStruct
			ray.Put(nilPtr)
		})
	})
}
