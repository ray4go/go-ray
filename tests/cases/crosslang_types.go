package cases

import (
	"github.com/ray4go/go-ray/ray"
	"fmt"
	"github.com/stretchr/testify/require"
)

// Test cross-language types conversion
func init() {
	AddTestCase("CrossLangTypes-Go2Py", func(assert *require.Assertions) {
		testcases := []struct {
			goVal       any
			pyExpresion string
		}{
			{0, "0"},
			{123, "123"},
			{-123, "-123"},
			{int64(2398298736389), "2398298736389"},
			{int32(1872397432), "1872397432"},
			{int16(6552), "6552"},
			{int8(123), "123"},
			{int8(-123), "-123"},
			{uint(123), "123"},
			{uint64(2389792875834), "2389792875834"},
			{uint32(897928), "897928"},
			{uint16(6552), "6552"},
			{uint8(123), "123"},

			{float32(123.0), "123.0"},
			{float64(123.456), "123.456"},
			{123.456, "123.456"},
			{-123.456, "-123.456"},

			{nil, "None"},
			{true, "True"},
			{false, "False"},
			{"hello", "'hello'"},
			{[]byte("hello"), `b"hello"`},
			{map[string]struct{}{"a": {}, "b": {}, "c": {}}, `{"a":{}, "b":{}, "c":{}}`},
			{[]string{"a", "b", "c"}, `["a", "b", "c"]`},
			{[]int{1, 2, 3}, `[1, 2, 3]`},
			{[]float64{1.1, 2.2, 3.3}, `[1.1, 2.2, 3.3]`},
			{[]bool{true, false, true}, `[True, False, True]`},
			{
				[]map[string]string{
					{"a": "1", "b": "2"},
					{"c": "3", "d": "4"},
				},
				`[{"a":"1", "b":"2"}, {"c":"3", "d":"4"}]`,
			},
			{
				[]map[string]int{
					{"a": 1, "b": 2},
					{"c": 3, "d": 4},
				},
				`[{"a":1, "b":2}, {"c":3, "d":4}]`,
			},
			{
				[]map[string]bool{
					{"a": true, "b": false},
					{"c": true, "d": false},
				},
				`[{"a":True, "b":False}, {"c":True, "d":False}]`,
			},
			{
				map[any]any{
					"str":    "hello",
					"bool":   true,
					"nil":    nil,
					"bytes":  []byte("hello"),
					"map":    map[string]int{"a": 1, "b": 2},
					"map2":   map[string]any{"a": 1, "b": false},
					"slice":  []int{1, 2, 3},
					"slice2": []any{1, false, "str"},
				},
				`{'str': 'hello', 'bool': True, 'nil': None, 'bytes': b'hello', 'map': {'b': 2, 'a': 1}, 'map2': {'a': 1, 'b': False}, 'slice': [1, 2, 3], 'slice2': [1, False, 'str']}`,
			},
			{
				struct {
					A int
					B string
					C bool
					D float64
					E []byte
					F []int
					G []string
					H []bool
					I []map[string]int
					J struct {
						X int
						Y string
						Z bool
					}
				}{
					A: 1,
					B: "str",
					C: true,
					D: 2.0,
					E: []byte("hello"),
					F: []int{1, 2, 3},
					G: []string{"a", "b", "c"},
					H: []bool{true, false, true},
					I: []map[string]int{
						{"a": 1, "b": 2},
						{"c": 3, "d": 4},
					},
					J: struct {
						X int
						Y string
						Z bool
					}{
						X: 1,
						Y: "str",
						Z: true,
					},
				},
				`{'A': 1, 'B': 'str', 'C': True, 'D': 2.0, 'E': b'hello', 'F': [1, 2, 3], 'G': ['a', 'b', 'c'], 'H': [True, False, True], 'I': [{'a': 1, 'b': 2}, {'d': 4, 'c': 3}], 'J': {'X': 1, 'Y': 'str', 'Z': True}}`,
			},

			{pointer(123), "123"},
			{pointer(123.456), "123.456"},
			{pointer(true), "True"},
			{pointer(false), "False"},
			{pointer("hello"), "'hello'"},
			{pointer([]byte("hello")), `b"hello"`},
			{pointer(map[string]struct{}{"a": {}, "b": {}, "c": {}}), `{"a":{}, "b":{}, "c":{}}`},
			{pointer([]string{"a", "b", "c"}), `["a", "b", "c"]`},
			{pointer([]int{1, 2, 3}), `[1, 2, 3]`},
			{pointer([]float64{1.1, 2.2, 3.3}), `[1.1, 2.2, 3.3]`},
			{pointer([]bool{true, false, true}), `[True, False, True]`},
			{
				pointer([]map[string]string{
					{"a": "1", "b": "2"},
					{"c": "3", "d": "4"},
				}),
				`[{"a":"1", "b":"2"}, {"c":"3", "d":"4"}]`,
			},
			{
				pointer(struct {
					A int
					B string
					C bool
				}{
					A: 1,
					B: "str",
					C: true,
				}),
				`{'A': 1, 'B':'str', 'C': True}`,
			},
		}

		var err error
		for _, testcase := range testcases {
			pycode := fmt.Sprintf(`def _(v): assert v == %s, f"{v!r} != pyval"`, testcase.pyExpresion)
			err = ray.CallPythonCode(pycode, testcase.goVal).GetInto()
			assert.NoError(err, "pyExpresion: %s, Expect Go val: %#v", testcase.pyExpresion, testcase.goVal)
		}
	})

	AddTestCase("CrossLangTypes-Py2Go", func(assert *require.Assertions) {
		testPyTypes2Go[int](`123`, 123, assert)
		testPyTypes2Go[uint](`123`, 123, assert)
		testPyTypes2Go[int](`None`, 0, assert)
		testPyTypes2Go[*int](`None`, nil, assert)

		testPyTypes2Go[float64](`123.456`, 123.456, assert)
		testPyTypes2Go[float64](`None`, 0.0, assert)
		testPyTypes2Go[*float64](`None`, nil, assert)

		testPyTypes2Go[bool](`True`, true, assert)
		testPyTypes2Go[bool](`False`, false, assert)
		testPyTypes2Go[bool](`None`, false, assert)
		testPyTypes2Go[*bool](`None`, nil, assert)

		testPyTypes2Go[map[string]int](`{b"key": 1}`, map[string]int{"key": 1}, assert)

		// error
		testPyTypes2GoOverflow[int8](`-1230`, assert)
	})
}

func pointer[T any](v T) *T {
	return &v
}

func testPyTypes2Go[T any](pyExpresion string, expect T, assert *require.Assertions) {
	var getinto T
	pycode := fmt.Sprintf(`def _(): return %s`, pyExpresion)
	err := ray.CallPythonCode(pycode).GetInto(&getinto)
	assert.NoError(err)
	assert.Equal(getinto, expect, "pyExpresion: %s", pyExpresion)
}

func testPyTypes2GoOverflow[T any](pyExpresion string, assert *require.Assertions) {
	defer func() {
		assert.Contains(recover().(string), "overflow")
	}()
	var t T
	testPyTypes2Go[T](pyExpresion, t, assert)
}
