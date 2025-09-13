package cases

import (
	"fmt"
	"math"

	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

// Test cross-language types conversion
func init() {
	AddTestCase("CrossLangTypes-Go2Py", func(assert *require.Assertions) {
		// Golang -> Python 类型转换
		testcases := []struct {
			goVal       any    // golang 中的值
			pyExpresion string // 转换到python中的值
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
				map[int]string{
					1: "one",
					2: "two",
					3: "three",
				},
				`{1:"one", 2:"two", 3:"three"}`,
			},
			{
				map[bool]string{
					true:  "true",
					false: "false",
				},
				`{True:"true", False:"false"}`,
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
		// Python -> Golang 类型转换
		// Golang 函数在接收Python传参和读取Python调用结果时，需要提前声明类型
		// testPyTypes2Go[T](pyExpresion, expect, assert)
		// 表示对于Python表达式 pyExpresion 中的值，若在golang中通过类型T接收时， 其值应该为 expect。

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

		testPyTypes2Go(`{b"key": 1}`, map[string]int{"key": 1}, assert)
		testPyTypes2Go(`{"key": 1}`, map[string]int{"key": 1}, assert)
		testPyTypes2Go(`{1: "one"}`, map[int]string{1: "one"}, assert)

		testPyTypes2Go[int64](`2**63-1`, 9223372036854775807, assert)
		testPyTypes2Go[uint64](`2**64-1`, 18446744073709551615, assert)

		// error
		testPyTypes2GoOverflow[int8](`-1230`, assert)
		assert.Panics(func() {
			testPyTypes2Go[uint8](`-1`, 0, assert)
		})
	})

	// Test Python to Go conversions for more complex types
	AddTestCase("CrossLangTypes-Py2Go-ExtendedTypes", func(assert *require.Assertions) {
		// Test string conversions
		testPyTypes2Go[string](`"hello world"`, "hello world", assert)
		testPyTypes2Go[string](`""`, "", assert)
		testPyTypes2Go[string](`"中文测试"`, "中文测试", assert)
		testPyTypes2Go[string](`"emoji 🚀"`, "emoji 🚀", assert)
		testPyTypes2Go[string](`None`, "", assert)
		testPyTypes2Go[*string](`None`, nil, assert)
		testPyTypes2Go[*string](`"test"`, pointer("test"), assert)

		// Test bytes conversions
		testPyTypes2Go[[]byte](`b"hello"`, []byte("hello"), assert)
		testPyTypes2Go[[]byte](`b""`, []byte{}, assert)
		testPyTypes2Go[[]byte](`b"\x00\x01\x02\x03"`, []byte{0, 1, 2, 3}, assert)
		testPyTypes2Go[[]byte](`None`, []byte(nil), assert)
		testPyTypes2Go[*[]byte](`None`, nil, assert)

		// Test various integer types with edge values
		testPyTypes2Go[int](`0`, 0, assert)
		testPyTypes2Go[int](`-1`, -1, assert)
		testPyTypes2Go[int8](`127`, int8(127), assert)
		testPyTypes2Go[int8](`-128`, int8(-128), assert)
		testPyTypes2Go[int16](`32767`, int16(32767), assert)
		testPyTypes2Go[int16](`-32768`, int16(-32768), assert)
		testPyTypes2Go[int32](`2147483647`, int32(2147483647), assert)
		testPyTypes2Go[int32](`-2147483648`, int32(-2147483648), assert)
		testPyTypes2Go[uint8](`255`, uint8(255), assert)
		testPyTypes2Go[uint16](`65535`, uint16(65535), assert)
		testPyTypes2Go[uint32](`4294967295`, uint32(4294967295), assert)

		// Test float types
		testPyTypes2Go[float32](`123.456`, float32(123.456), assert)
		testPyTypes2Go[float32](`0.0`, float32(0.0), assert)
		testPyTypes2Go[float32](`-123.456`, float32(-123.456), assert)
		testPyTypes2Go[float64](`1.7976931348623157e+308`, 1.7976931348623157e+308, assert)

		// Test slices
		testPyTypes2Go[[]int](`[1, 2, 3, 4, 5]`, []int{1, 2, 3, 4, 5}, assert)
		testPyTypes2Go[[]int](`[]`, []int{}, assert)
		testPyTypes2Go[[]string](`["a", "b", "c"]`, []string{"a", "b", "c"}, assert)
		testPyTypes2Go[[]float64](`[1.1, 2.2, 3.3]`, []float64{1.1, 2.2, 3.3}, assert)
		testPyTypes2Go[[]bool](`[True, False, True]`, []bool{true, false, true}, assert)
		testPyTypes2Go[[]int](`None`, []int(nil), assert)

		// Test maps
		testPyTypes2Go[map[string]int](`{"a": 1, "b": 2}`, map[string]int{"a": 1, "b": 2}, assert)
		testPyTypes2Go[map[string]string](`{"key": "value"}`, map[string]string{"key": "value"}, assert)
		testPyTypes2Go[map[string]bool](`{"true": True, "false": False}`, map[string]bool{"true": true, "false": false}, assert)
		testPyTypes2Go[map[string]int](`{}`, map[string]int{}, assert)
		testPyTypes2Go[map[string]int](`None`, map[string]int(nil), assert)

		// Test nested structures
		testPyTypes2Go[[][]int](`[[1, 2], [3, 4], [5, 6]]`, [][]int{{1, 2}, {3, 4}, {5, 6}}, assert)
		testPyTypes2Go[map[string][]int](`{"nums": [1, 2, 3]}`, map[string][]int{"nums": {1, 2, 3}}, assert)
		testPyTypes2Go[[]map[string]int](`[{"a": 1}, {"b": 2}]`, []map[string]int{{"a": 1}, {"b": 2}}, assert)

		// Test pointers with various types
		testPyTypes2Go[*int](`123`, pointer(123), assert)
		testPyTypes2Go[*float64](`123.456`, pointer(123.456), assert)
		testPyTypes2Go[*bool](`True`, pointer(true), assert)
	})

	// Test Go to Python conversions for edge cases and complex types
	AddTestCase("CrossLangTypes-Go2Py-EdgeCases", func(assert *require.Assertions) {
		// Test numeric edge cases
		testcases := []struct {
			goVal       any
			pyExpresion string
		}{
			// Integer edge values
			{int8(127), "127"},
			{int8(-128), "-128"},
			{int16(32767), "32767"},
			{int16(-32768), "-32768"},
			{int32(2147483647), "2147483647"},
			{int32(-2147483648), "-2147483648"},
			{uint8(255), "255"},
			{uint16(65535), "65535"},
			{uint32(4294967295), "4294967295"},
			{uint64(18446744073709551615), "18446744073709551615"},

			// Float edge values
			{float32(0.0), "0.0"},
			{math.Copysign(0, -1), "-0.0"},
			{float64(1.7976931348623157e+308), "1.7976931348623157e+308"},
			{float64(-1.7976931348623157e+308), "-1.7976931348623157e+308"},

			// String edge cases
			{"", "''"},
			{" ", "' '"},
			{"\n\t\r", `'\n\t\r'`},
			{"unicode: 中文 🌟", "'unicode: 中文 🌟'"},

			// Byte slice edge cases
			{[]byte{}, "b''"},
			{[]byte{0, 255}, "b'\\x00\\xff'"},
			{[]byte("binary\x00data"), "b'binary\\x00data'"},

			// Empty collections
			{[]int{}, "[]"},
			{[]string{}, "[]"},
			{map[string]int{}, "{}"},
			{[]map[string]int{}, "[]"},
			{map[string][]int{}, "{}"},

			// Collections with nil/None elements
			{[]*int{nil, pointer(1), nil}, "[None, 1, None]"},
			{[]*string{nil, pointer("test"), nil}, "[None, 'test', None]"},
			{map[string]*int{"a": nil, "b": pointer(1)}, "{'a': None, 'b': 1}"},

			// Deeply nested structures
			{
				[][]int{{1, 2}, {3, 4}, {5, 6}},
				"[[1, 2], [3, 4], [5, 6]]",
			},
			{
				map[string][]int{"even": {2, 4, 6}, "odd": {1, 3, 5}},
				"{'even': [2, 4, 6], 'odd': [1, 3, 5]}",
			},
			{
				[]map[string]int{{"a": 1, "b": 2}, {"c": 3, "d": 4}},
				"[{'a': 1, 'b': 2}, {'c': 3, 'd': 4}]",
			},

			// Complex structs
			{
				struct {
					Name     string
					Age      *int
					Active   bool
					Metadata map[string]any
					Tags     []string
					Scores   []float64
				}{
					Name:     "test user",
					Age:      pointer(25),
					Active:   true,
					Metadata: map[string]any{"role": "admin", "level": 5},
					Tags:     []string{"important", "verified"},
					Scores:   []float64{95.5, 87.3, 92.1},
				},
				"{'Name': 'test user', 'Age': 25, 'Active': True, 'Metadata': {'role': 'admin', 'level': 5}, 'Tags': ['important', 'verified'], 'Scores': [95.5, 87.3, 92.1]}",
			},

			// Struct with nested struct
			{
				struct {
					User struct {
						ID   int
						Name string
					}
					Settings struct {
						Theme string
						Count int
					}
				}{
					User: struct {
						ID   int
						Name string
					}{ID: 123, Name: "john"},
					Settings: struct {
						Theme string
						Count int
					}{Theme: "dark", Count: 10},
				},
				"{'User': {'ID': 123, 'Name': 'john'}, 'Settings': {'Theme': 'dark', 'Count': 10}}",
			},
		}

		for _, testcase := range testcases {
			pycode := fmt.Sprintf(`def _(v): assert v == %s, f"{v!r} != "+str(%s)`, testcase.pyExpresion, testcase.pyExpresion)
			err := ray.CallPythonCode(pycode, testcase.goVal).GetInto()
			assert.NoError(err, "pyExpresion: %s, Go val: %#v", testcase.pyExpresion, testcase.goVal)
		}
	})

	// Test Python to Go struct conversion
	AddTestCase("CrossLangTypes-Py2Go-StructConversion", func(assert *require.Assertions) {
		// Test simple struct conversion
		type SimpleStruct struct {
			Name  string
			Age   int
			Valid bool
		}

		var simple SimpleStruct
		pycode := `def _(): return {"Name": "Alice", "Age": 30, "Valid": True}`
		err := ray.CallPythonCode(pycode).GetInto(&simple)
		assert.NoError(err)
		assert.Equal(SimpleStruct{Name: "Alice", Age: 30, Valid: true}, simple)

		// Test struct with pointer fields
		type StructWithPointers struct {
			Name   *string
			Age    *int
			Active *bool
			Score  *float64
		}

		// Test with all fields present
		var withPtrs1 StructWithPointers
		pycode = `def _(): return {"Name": "Bob", "Age": 25, "Active": True, "Score": 95.5}`
		err = ray.CallPythonCode(pycode).GetInto(&withPtrs1)
		assert.NoError(err)
		assert.Equal("Bob", *withPtrs1.Name)
		assert.Equal(25, *withPtrs1.Age)
		assert.Equal(true, *withPtrs1.Active)
		assert.Equal(95.5, *withPtrs1.Score)

		// Test with some fields as None
		var withPtrs2 StructWithPointers
		pycode = `def _(): return {"Name": "Charlie", "Age": None, "Active": False, "Score": None}`
		err = ray.CallPythonCode(pycode).GetInto(&withPtrs2)
		assert.NoError(err)
		assert.Equal("Charlie", *withPtrs2.Name)
		assert.Nil(withPtrs2.Age)
		assert.Equal(false, *withPtrs2.Active)
		assert.Nil(withPtrs2.Score)

		// Test nested struct
		type NestedStruct struct {
			Outer struct {
				Inner struct {
					Value string
					Count int
				}
				Name string
			}
			ID int
		}

		var nested NestedStruct
		pycode = `def _(): return {"Outer": {"Inner": {"Value": "deep", "Count": 42}, "Name": "middle"}, "ID": 100}`
		err = ray.CallPythonCode(pycode).GetInto(&nested)
		assert.NoError(err)
		assert.Equal("deep", nested.Outer.Inner.Value)
		assert.Equal(42, nested.Outer.Inner.Count)
		assert.Equal("middle", nested.Outer.Name)
		assert.Equal(100, nested.ID)

		// Test struct with slice and map fields
		type ComplexStruct struct {
			Tags     []string
			Scores   []int
			Metadata map[string]string
			Numbers  map[string]int
		}

		var complex ComplexStruct
		pycode = `def _(): return {"Tags": ["tag1", "tag2"], "Scores": [1, 2, 3], "Metadata": {"key": "value"}, "Numbers": {"one": 1, "two": 2}}`
		err = ray.CallPythonCode(pycode).GetInto(&complex)
		assert.NoError(err)
		assert.Equal([]string{"tag1", "tag2"}, complex.Tags)
		assert.Equal([]int{1, 2, 3}, complex.Scores)
		assert.Equal(map[string]string{"key": "value"}, complex.Metadata)
		assert.Equal(map[string]int{"one": 1, "two": 2}, complex.Numbers)
	})

	// Test overflow and error conditions
	AddTestCase("CrossLangTypes-Py2Go-ErrorCases", func(assert *require.Assertions) {
		// Test integer overflow cases
		testPyTypes2GoOverflow[int8](`128`, assert)
		testPyTypes2GoOverflow[int8](`-129`, assert)
		testPyTypes2GoOverflow[uint8](`256`, assert)
		testPyTypes2GoOverflow[int16](`32768`, assert)
		testPyTypes2GoOverflow[int16](`-32769`, assert)
		testPyTypes2GoOverflow[uint16](`65536`, assert)
		testPyTypes2GoOverflow[int32](`2147483648`, assert)
		testPyTypes2GoOverflow[int32](`-2147483649`, assert)
		testPyTypes2GoOverflow[uint32](`4294967296`, assert)
	})

	// Test bidirectional consistency
	AddTestCase("CrossLangTypes-Bidirectional", func(assert *require.Assertions) {
		// Test that Go -> Python -> Go preserves values
		testValues := []any{
			42,
			3.14159,
			true,
			"hello world",
			[]byte("binary data"),
			[]int{1, 2, 3, 4, 5},
			[]string{"a", "b", "c"},
			map[string]int{"one": 1, "two": 2, "three": 3},
			struct {
				Name  string
				Count int
				Valid bool
			}{Name: "test", Count: 100, Valid: true},
		}

		for i, original := range testValues {
			// Send Go value to Python and get it back
			pycode := `def _(v): return v` // Python identity function

			switch v := original.(type) {
			case int:
				var result int
				err := ray.CallPythonCode(pycode, v).GetInto(&result)
				assert.NoError(err, "Test case %d failed", i)
				assert.Equal(v, result, "Test case %d: values don't match", i)
			case float64:
				var result float64
				err := ray.CallPythonCode(pycode, v).GetInto(&result)
				assert.NoError(err, "Test case %d failed", i)
				assert.Equal(v, result, "Test case %d: values don't match", i)
			case bool:
				var result bool
				err := ray.CallPythonCode(pycode, v).GetInto(&result)
				assert.NoError(err, "Test case %d failed", i)
				assert.Equal(v, result, "Test case %d: values don't match", i)
			case string:
				var result string
				err := ray.CallPythonCode(pycode, v).GetInto(&result)
				assert.NoError(err, "Test case %d failed", i)
				assert.Equal(v, result, "Test case %d: values don't match", i)
			case []byte:
				var result []byte
				err := ray.CallPythonCode(pycode, v).GetInto(&result)
				assert.NoError(err, "Test case %d failed", i)
				assert.Equal(v, result, "Test case %d: values don't match", i)
			case []int:
				var result []int
				err := ray.CallPythonCode(pycode, v).GetInto(&result)
				assert.NoError(err, "Test case %d failed", i)
				assert.Equal(v, result, "Test case %d: values don't match", i)
			case []string:
				var result []string
				err := ray.CallPythonCode(pycode, v).GetInto(&result)
				assert.NoError(err, "Test case %d failed", i)
				assert.Equal(v, result, "Test case %d: values don't match", i)
			case map[string]int:
				var result map[string]int
				err := ray.CallPythonCode(pycode, v).GetInto(&result)
				assert.NoError(err, "Test case %d failed", i)
				assert.Equal(v, result, "Test case %d: values don't match", i)
			default:
				// For complex types like structs, we can't easily do type assertion
				// so we skip them in this particular test
				continue
			}
		}
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
	var t T
	defer func() {
		assert.Containsf(recover().(string), "overflow", "pyExpresion: %s, go val: %#v", pyExpresion, t)
	}()
	testPyTypes2Go[T](pyExpresion, t, assert)
}
