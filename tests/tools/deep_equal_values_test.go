package tools

import (
	"testing"
)

func TestDeepEqualValues(t *testing.T) {
	testCases := []struct {
		name     string
		a        any
		b        any
		expected bool
	}{
		// 1. 基本类型
		{name: "Basic/Int: same type and value", a: int(1), b: int(1), expected: true},
		{name: "Basic/Int: different type, same value", a: int(1), b: int64(1), expected: true},
		{name: "Basic/Int: signed vs unsigned, same value", a: uint8(12), b: int32(12), expected: true},
		{name: "Basic/Int: different values", a: int(1), b: int(2), expected: false},
		{name: "Basic/Float: same type and value", a: float64(3.14), b: float64(3.14), expected: true},
		{name: "Basic/Float: different type, same value", a: float32(3.14), b: float64(3.14), expected: true},
		{name: "Basic/Float: different values", a: float64(1.0), b: float64(2.0), expected: false},
		{name: "Basic/String: same", a: "hello", b: "hello", expected: true},
		{name: "Basic/String: different", a: "hello", b: "world", expected: false},
		{name: "Basic/Bool: same", a: true, b: true, expected: true},
		{name: "Basic/Bool: different", a: true, b: false, expected: false},
		{name: "Basic/Any: wrapped different int types", a: any(int(5)), b: any(int16(5)), expected: true},

		// 2. 切片 (Slices)
		{name: "Slice/Identical", a: []int{1, 2, 3}, b: []int{1, 2, 3}, expected: true},
		{name: "Slice/Different element types, same values", a: []int{1, 2, 3}, b: []int64{1, 2, 3}, expected: true},
		{name: "Slice/User example: any vs int", a: []any{int64(1), int64(44), 2}, b: []int{1, 44, 2}, expected: true},
		{name: "Slice/Any slice with mixed underlying types", a: []any{1, "a", true}, b: []any{int32(1), "a", true}, expected: true},
		{name: "Slice/Different element values", a: []int{1, 2, 3}, b: []int{1, 2, 4}, expected: false},
		{name: "Slice/Different order", a: []int{1, 2, 3}, b: []int{3, 2, 1}, expected: false},
		{name: "Slice/Different length", a: []int{1, 2}, b: []int{1, 2, 3}, expected: false},
		{name: "Slice/Boundary: two empty slices", a: []int{}, b: []string{}, expected: true},
		{name: "Slice/Boundary: nil vs empty", a: []int(nil), b: []int{}, expected: true},
		{name: "Slice/Boundary: nil vs nil, different types", a: []int(nil), b: []string(nil), expected: true},
		{name: "Slice/One nil, one not", a: []int(nil), b: []int{1}, expected: false},

		// 3. 映射 (Maps)
		{name: "Map/Identical", a: map[string]int{"a": 1}, b: map[string]int{"a": 1}, expected: true},
		{name: "Map/User example: map[string]int vs map[any]any", a: map[string]int{"a": 1}, b: map[any]any{"a": 1}, expected: true},
		{name: "Map/Different key/value types, same values", a: map[string]int{"a": 1, "b": 2}, b: map[string]int64{"b": 2, "a": 1}, expected: true},
		{name: "Map/Different key types, same values", a: map[int]string{1: "a"}, b: map[int8]string{1: "a"}, expected: true},
		{name: "Map/Different values", a: map[string]int{"a": 1}, b: map[string]int{"a": 2}, expected: false},
		{name: "Map/Different keys", a: map[string]int{"a": 1}, b: map[string]int{"b": 1}, expected: false},
		{name: "Map/Different number of keys", a: map[string]int{"a": 1}, b: map[string]int{"a": 1, "b": 2}, expected: false},
		{name: "Map/Boundary: two empty maps", a: map[string]int{}, b: map[int]bool{}, expected: true},
		{name: "Map/Boundary: nil vs empty", a: map[string]int(nil), b: map[string]int{}, expected: true},
		{name: "Map/Boundary: nil vs nil, different types", a: map[string]int(nil), b: map[any]any(nil), expected: true},

		// 4. 嵌套结构
		{name: "Nested/Map with slice values", a: map[string][]int{"a": {1, 2}}, b: map[string][]int64{"a": {1, 2}}, expected: true},
		{name: "Nested/Complex: map[any][]any vs map[string][]any", a: map[any][]any{"key": {1, "a"}}, b: map[string][]any{"key": {int16(1), "a"}}, expected: true},
		{name: "Nested/Slice of maps", a: []map[string]int{{"a": 1}, {"b": 2}}, b: []map[string]int8{{"a": 1}, {"b": 2}}, expected: true},
		{name: "Nested/Slice of slices", a: [][]int{{1}, {2, 3}}, b: [][]int64{{1}, {2, 3}}, expected: true},
		{name: "Nested/Map with slice values, different content", a: map[string][]int{"a": {1, 99}}, b: map[string][]int64{"a": {1, 2}}, expected: false},
		{name: "Nested/Slice of maps, different keys", a: []map[string]int{{"a": 1}}, b: []map[string]int{{"b": 1}}, expected: false},

		// 5. 失败与边界场景
		{name: "Fail/Int vs Float", a: int(1), b: float64(1.0), expected: false},
		{name: "Fail/Int vs String", a: int(1), b: "1", expected: false},
		{name: "Fail/Any Int vs Any Float", a: any(int(1)), b: any(float64(1.0)), expected: false},
		{name: "Fail/Different Kinds: Slice vs Map", a: []int{1}, b: map[int]int{1: 1}, expected: false},
		{name: "Fail/Value vs nil interface", a: 1, b: nil, expected: false},
		{name: "Fail/Value vs nil typed slice", a: 1, b: []int(nil), expected: false},
		{name: "Fail/Empty slice vs nil interface", a: []int{}, b: nil, expected: false},
		{name: "Fail/Empty map vs nil interface", a: map[string]int{}, b: nil, expected: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := DeepEqualValues(tc.a, tc.b)
			if result != tc.expected {
				t.Errorf("DeepEqualValues(%#v, %#v) = %v; want %v", tc.a, tc.b, result, tc.expected)
			}
		})
	}
}
