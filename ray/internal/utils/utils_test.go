package utils

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
	"unsafe"
)

// encodeBytesUnitsForTest is a helper function to create test data.
// It's the inverse of DecodeBytesUnits.
func encodeBytesUnitsForTest(units [][]byte) []byte {
	var buf bytes.Buffer
	lenBytes := make([]byte, 8)
	for _, unit := range units {
		// Write length
		binary.LittleEndian.PutUint64(lenBytes, uint64(len(unit)))
		buf.Write(lenBytes)
		// Write data
		buf.Write(unit)
	}
	return buf.Bytes()
}

// TestDecodeBytesUnits contains all test cases for the function.
func TestDecodeBytesUnits(t *testing.T) {
	testCases := []struct {
		name     string
		input    []byte
		expected [][]byte
	}{
		{
			name: "✅ Normal Case: Multiple Units",
			input: encodeBytesUnitsForTest([][]byte{
				[]byte("hello"),
				[]byte("world"),
			}),
			expected: [][]byte{
				[]byte("hello"),
				[]byte("world"),
			},
		},
		{
			name:     "✅ Edge Case: Empty Input",
			input:    []byte{},
			expected: [][]byte{},
		},
		{
			name:     "✅ Edge Case: Nil Input",
			input:    nil,
			expected: [][]byte{},
		},
		{
			name: "✅ Edge Case: Single Unit",
			input: encodeBytesUnitsForTest([][]byte{
				[]byte("single unit test"),
			}),
			expected: [][]byte{
				[]byte("single unit test"),
			},
		},
		{
			name: "✅ Edge Case: Unit with Zero Length",
			input: encodeBytesUnitsForTest([][]byte{
				[]byte("first"),
				[]byte(""), // Empty unit
				[]byte("third"),
			}),
			expected: [][]byte{
				[]byte("first"),
				[]byte(""),
				[]byte("third"),
			},
		},
		{
			name:     "❌ Malformed: Truncated Length",
			input:    []byte{0, 0, 0, 0, 0, 0, 0}, // Only 7 bytes, less than length prefix
			expected: [][]byte{},
		},
		{
			name: "❌ Malformed: Truncated Data",
			// Declares a length of 10, but provides only 5 bytes of data.
			input:    append([]byte{0, 0, 0, 0, 0, 0, 0, 10}, []byte("12345")...),
			expected: [][]byte{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, _ := DecodeBytesUnits(tc.input)

			// When expecting an empty slice, check if the result is also empty.
			if len(tc.expected) == 0 && len(result) == 0 {
				return // Test passes
			}

			// For non-empty cases, use DeepEqual for a robust comparison.
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("DecodeBytesUnits() = %v, want %v", result, tc.expected)
			}
		})
	}
}

// TestIsNilPointer 测试 IsNilTypePointer 函数的各种场景
func TestIsNilPointer(t *testing.T) {
	// 辅助函数，用于格式化测试名称
	testCase := func(name string, input any, expected bool) {
		t.Run(name, func(t *testing.T) {
			got := IsNilTypePointer(input)
			if got != expected {
				t.Errorf("IsNilTypePointer(%v) = %v, want %v (type: %T, reflect.Kind: %s)", input, got, expected, input, reflect.ValueOf(input).Kind().String())
			}
		})
	}

	// 1. 测试显式传入的nil
	testCase("ExplicitNilInterface", nil, true)

	// 2. 测试各种nil指针
	var intPtr *int
	testCase("NilIntPtr", intPtr, true)

	var stringPtr *string
	testCase("NilStringPtr", stringPtr, true)

	var customTypePtr *struct{}
	testCase("NilCustomTypePtr", customTypePtr, true)

	// interface{}类型的nil值，其内部类型和值都为nil
	var nilInterface any
	testCase("NilAnyInterface", nilInterface, true)

	// 函数指针
	var nilFuncPtr func()
	testCase("NilFuncPtr", nilFuncPtr, false)

	// 切片（尽管IsNilPointer没有处理切片，但reflect.ValueOf(nilSlice).IsNil() 返回true）
	// 但是根据你的函数逻辑，IsNilTypePointer 会返回false，因为kind不是Ptr或Interface
	var nilSlice []int
	testCase("NilSliceLiteralShouldBeFalse", nilSlice, false) // Kind is Slice, not Ptr or Interface

	// map
	var nilMap map[string]int
	testCase("NilMapLiteralShouldBeFalse", nilMap, false) // Kind is Map, not Ptr or Interface

	// channel
	var nilChan chan int
	testCase("NilChanLiteralShouldBeFalse", nilChan, false) // Kind is Chan, not Ptr or Interface

	// 3. 测试非nil指针
	myInt := 10
	intPtr = &myInt
	testCase("NonNullIntPtr", intPtr, false)

	myString := "hello"
	stringPtr = &myString
	testCase("NonNullStringPtr", stringPtr, false)

	myStruct := struct{}{}
	customTypePtr = &myStruct
	testCase("NonNullCustomTypePtr", customTypePtr, false)

	// 4. 测试非nil接口，其底层值是nil（例如: var err error; return err）
	// 这是一个非常重要的边缘情况：非nil接口，但其内部值是nil
	// func example() error { return nil }
	// var err error = example() // 此时err接口本身是非nil的，但其内部值是nil
	// IsNilTypePointer(err) 应该返回 true
	var err error
	var eof error = nil // 通常是io.EOF这样的错误，但这里模拟nil
	err = eof           // 将nil赋值给一个error接口变量，此时接口类型为nil，值为nil
	testCase("NonNullInterfaceWithNilUnderlyingValue", err, true)

	// 5. 测试非nil接口，其底层值也是非nil
	var nonNilError error = &CustomError{} // 自定义错误，非nil
	testCase("NonNullInterfaceWithNonNullUnderlyingValue", nonNilError, false)

	var ptr *CustomError
	testCase("Ptr", ptr, true)

	// 6. 测试非指针和非接口类型
	testCase("IntValue", 123, false)
	testCase("StringValue", "test", false)
	testCase("BoolValue", true, false)
	testCase("SliceValue", []int{1, 2, 3}, false)
	testCase("MapValue", map[string]int{"a": 1}, false)
	testCase("StructValue", struct{ Field string }{Field: "test"}, false)
	testCase("FuncValue", func() {}, false)
	// Zero value of pointer type
	var zeroIntPtr *int // Same as var intPtr *int; implicit initialization to nil
	testCase("ZeroValueIntPtr", zeroIntPtr, true)

	// Unsafe.Pointer
	// Unsafe.Pointer 的 Kind() 是 reflect.UnsafePointer
	// 所以它不会被认为是 Ptr 类型，应该返回 false
	var unsafePtr unsafe.Pointer
	testCase("UnsafePointerNil", unsafePtr, false) // Kind is reflect.UnsafePointer
	dummyVar := 1
	unsafePtr = unsafe.Pointer(&dummyVar)
	testCase("UnsafePointerNonNull", unsafePtr, false) // Kind is reflect.UnsafePointer

	// 指向接口的指针
	var val any = "hello"
	var ptrToAny *any = &val
	testCase("PtrToAnyWithNonNullValue", ptrToAny, false) // 是 *any -> Ptr
	var nilVal any
	var ptrToNilAny *any = &nilVal
	testCase("PtrToAnyWithNilValue", ptrToNilAny, false) // 是 *any -> Ptr
	var nilPtrToAny *any                                 // nil pointer to any
	testCase("NilPtrToAny", nilPtrToAny, true)           // 是 *any -> Ptr
}

type CustomError struct{}

func (e *CustomError) Error() string {
	return "custom error"
}
