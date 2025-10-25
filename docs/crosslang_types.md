# GoRay Cross-Language Call Type Conversion Guide

Parameters and return values for GoRay's cross-language calls are serialized and deserialized between Python and Go using [msgpack](https://msgpack.org/).

Supported types include: integer, float, boolean, string, binary, list (Go slice), dictionary (Go map), and None (Go nil).
For dictionaries, keys must be of string or integer type.
Go additionally supports struct and pointer types. Go structs are converted to Python dictionaries.

## Go -> Python Type Conversion

Go to Python type conversion occurs when:
1.  Go calls a Python function (for parameter values).
2.  A Python call to a Go function returns (for return values).

The following table lists the type conversion rules from Go to Python:

| Go Type      | Converted Python Type              |
|--------------|------------------------------------|
| Integer      | `int`                              |
| Float        | `float`                            |
| `bool`       | `bool`                             |
| `string`     | `str`                              |
| `[]byte`     | `bytes`                            |
| Slice/Array  | `list`                             |
| `map`        | `dict`                             |
| `nil`        | `None`                             |
| `struct`     | `dict` (keys are Go struct field names) |

A Go pointer type is converted just like its underlying type. For example, a Go `*string` is converted to a Python `str`.

The Go `map[T]struct{}` type, often used as a Set, is converted to a Python `dict`:
`map[string]struct{}{"a": {}, "b": {}, "c": {}}` -> `{"a":{}, "b":{}, "c":{}}`

For specific conversion examples, please see the [test cases](../tests/cases/crosslang_types.go).

## Python -> Go Type Conversion

Python to Go type conversion occurs when:
1.  Python calls a Go function (for parameter values).
2.  A Go call to a Python function returns (for return values).

Since Go functions must declare types for incoming Python parameters and return values, the table below lists the compatible Go types for each Python type.

| Python Type | Compatible Go Types                                  |
|-------------|------------------------------------------------------|
| `int`       | Integer types and their pointers (must not overflow), `any` |
| `float`     | Float types and their pointers (must not overflow), `any`   |
| `bool`      | `bool`, `*bool`, `any`                               |
| `str`       | `string`, `*string`, `any`                           |
| `bytes`     | `[]byte`, `*[]byte`, `any`                           |
| `list`      | slice, `*slice`                                      |
| `dict`      | `map`, `*map`, `struct`, `*struct`                   |
| `None`      | Pointer types, or any non-`any` type (see explanation below) |

Note on converting Python's `None` type:

- If the target Go type is a pointer type, Python's `None` is converted to a `nil` pointer.
- If the target Go type is a non-pointer, non-interface type, Python's `None` is converted to the zero value of that type.