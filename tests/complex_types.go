package main

import (
	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

// Test complex data structures
type ComplexStruct struct {
	ID       int64
	Name     string
	Tags     []string
	Metadata map[string]interface{}
	Nested   NestedStruct
}

type NestedStruct struct {
	Value   float64
	Active  bool
	Details []int
}

func (_ testTask) ProcessComplexStruct(data ComplexStruct) ComplexStruct {
	data.ID *= 2
	data.Name = "processed_" + data.Name
	data.Tags = append(data.Tags, "processed")
	data.Metadata["processed"] = true
	data.Nested.Value *= 1.5
	data.Nested.Active = !data.Nested.Active
	data.Nested.Details = append(data.Nested.Details, 999)
	return data
}

func (_ testTask) ProcessSliceOfStructs(structs []ComplexStruct) []ComplexStruct {
	result := make([]ComplexStruct, len(structs))
	for i, s := range structs {
		result[i] = ComplexStruct{
			ID:   s.ID + int64(i),
			Name: s.Name + "_batch",
			Tags: append(s.Tags, "batch_processed"),
			Metadata: map[string]interface{}{
				"batch_index": i,
				"original_id": s.ID,
			},
			Nested: NestedStruct{
				Value:   s.Nested.Value + float64(i),
				Active:  s.Nested.Active,
				Details: append(s.Nested.Details, i),
			},
		}
	}
	return result
}

func (_ testTask) ProcessMap(data map[string]int) map[string]int {
	result := make(map[string]int)
	for k, v := range data {
		result[k+"_processed"] = v * 2
	}
	return result
}

func init() {
	addTestCase("TestComplexStruct", func(assert *require.Assertions) {
		input := ComplexStruct{
			ID:   123,
			Name: "test",
			Tags: []string{"tag1", "tag2"},
			Metadata: map[string]interface{}{
				"version": "1.0",
				"author":  "test_user",
			},
			Nested: NestedStruct{
				Value:   3.14,
				Active:  true,
				Details: []int{1, 2, 3},
			},
		}

		objRef := ray.RemoteCall("ProcessComplexStruct", input)
		result, err := objRef.Get1()
		assert.Nil(err)

		expected := ComplexStruct{
			ID:   246,
			Name: "processed_test",
			Tags: []string{"tag1", "tag2", "processed"},
			Metadata: map[string]interface{}{
				"version":   "1.0",
				"author":    "test_user",
				"processed": true,
			},
			Nested: NestedStruct{
				Value:   4.71,
				Active:  false,
				Details: []int{1, 2, 3, 999},
			},
		}

		assert.Equal(expected, result)
	})

	addTestCase("TestSliceOfStructs", func(assert *require.Assertions) {
		input := []ComplexStruct{
			{
				ID:   1,
				Name: "first",
				Tags: []string{"tag1"},
				Metadata: map[string]interface{}{
					"index": 0,
				},
				Nested: NestedStruct{
					Value:   1.0,
					Active:  true,
					Details: []int{10},
				},
			},
			{
				ID:   2,
				Name: "second",
				Tags: []string{"tag2"},
				Metadata: map[string]interface{}{
					"index": 1,
				},
				Nested: NestedStruct{
					Value:   2.0,
					Active:  false,
					Details: []int{20, 21},
				},
			},
		}

		objRef := ray.RemoteCall("ProcessSliceOfStructs", input)
		result, err := objRef.Get1()
		assert.Nil(err)

		resultSlice := result.([]ComplexStruct)
		assert.Len(resultSlice, 2)

		// Check first element
		assert.Equal(int64(1), resultSlice[0].ID)
		assert.Equal("first_batch", resultSlice[0].Name)
		assert.Contains(resultSlice[0].Tags, "batch_processed")
		assert.Equal(0, resultSlice[0].Metadata["batch_index"])
		assert.Equal(int64(1), resultSlice[0].Metadata["original_id"])

		// Check second element
		assert.Equal(int64(3), resultSlice[1].ID)
		assert.Equal("second_batch", resultSlice[1].Name)
		assert.Contains(resultSlice[1].Tags, "batch_processed")
		assert.Equal(1, resultSlice[1].Metadata["batch_index"])
		assert.Equal(int64(2), resultSlice[1].Metadata["original_id"])
	})

	addTestCase("TestMapProcessing", func(assert *require.Assertions) {
		input := map[string]int{
			"apple":  5,
			"banana": 3,
			"cherry": 8,
		}

		objRef := ray.RemoteCall("ProcessMap", input)
		result, err := objRef.Get1()
		assert.Nil(err)

		resultMap := result.(map[string]int)
		assert.Equal(10, resultMap["apple_processed"])
		assert.Equal(6, resultMap["banana_processed"])
		assert.Equal(16, resultMap["cherry_processed"])
		assert.Len(resultMap, 3)
	})
}
