package ray

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

// encodeBytesUnitsForTest is a helper function to create test data.
// It's the inverse of decodeBytesUnits.
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
			result, _ := decodeBytesUnits(tc.input)

			// When expecting an empty slice, check if the result is also empty.
			if len(tc.expected) == 0 && len(result) == 0 {
				return // Test passes
			}

			// For non-empty cases, use DeepEqual for a robust comparison.
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("decodeBytesUnits() = %v, want %v", result, tc.expected)
			}
		})
	}
}
