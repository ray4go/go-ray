package ray

import (
	"bytes"
	"github.com/ray4go/go-ray/ray/utils/log"
	"encoding/binary"
	"fmt"
	"go/token"
	"reflect"
	"sort"
)

// decodeBytesUnits decodes byte units from a single bytes.
// Each unit has the format: | length:8byte:int64 | data:${length}byte:[]byte |
// It returns a slice of bytes, where each bytes is a data unit.
func decodeBytesUnits(data []byte) ([][]byte, bool) {
	var units [][]byte
	offset := 0

	// Loop as long as there's potentially a full unit left.
	for offset < len(data) {
		// 1. Decode length prefix (8 bytes)
		// Check if there are enough bytes for the length prefix.
		if len(data) < offset+8 {
			// Not enough data for a length prefix, so we stop.
			break
		}

		// Convert 8 bytes to an integer. We use LittleEndian
		length := int(binary.LittleEndian.Uint64(data[offset : offset+8]))

		// Move offset past the length prefix.
		offset += 8

		// 2. Decode data part
		// Check if the slice contains the full data unit as declared by the length.
		if len(data) < offset+length {
			// The data is truncated; the declared length exceeds the remaining bytes.
			break
		}

		// Extract the data unit.
		unit := data[offset : offset+length]
		units = append(units, unit)

		// Move offset to the beginning of the next unit.
		offset += length
	}
	return units, offset == len(data)
}

func appendBytesUnit(buffer *bytes.Buffer, data []byte) {
	// 1. Encode length prefix (8 bytes)
	length := make([]byte, 8)
	binary.LittleEndian.PutUint64(length, uint64(len(data)))
	buffer.Write(length)
	// 2. Append the actual data
	buffer.Write(data)
}

func getExportedMethods(typ reflect.Type) []reflect.Method {
	methods := make([]reflect.Method, 0)
	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i) // deterministic: sort in lexicographic order.
		mtype := method.Type
		// callableType must be exported.
		ok := method.IsExported()
		// All arguments must be exported or builtin types.
		for j := 1; j < mtype.NumIn(); j++ {
			argType := mtype.In(j)
			if !isExportedOrBuiltinType(argType) {
				log.Debug("[Go] method %v arg %v is not exported\n", method, argType)
				ok = false
				break
			}
		}
		// all return values must be exported or builtin types.
		for j := 0; j < mtype.NumOut(); j++ {
			argType := mtype.Out(j)
			if !isExportedOrBuiltinType(argType) {
				ok = false
				break
			}
		}
		if ok {
			methods = append(methods, method)
		} else {
			log.Debug("[Go] skip method %v\n", method)
		}
	}
	return methods
}

// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return token.IsExported(t.Name()) || t.PkgPath() == ""
}

// getFuncReturnType 通过函数名获取函数的返回值类型
func getFuncReturnType(fn any) reflect.Type {
	// 1. 使用 reflect.ValueOf 获取函数的 Value
	fnValue := reflect.ValueOf(fn)

	// 2. 检查获取到的 Value 是否是函数
	if fnValue.Kind() != reflect.Func {
		return nil // 或者 panic("Input is not a function")
	}

	// 3. 获取函数的 Type
	fnType := fnValue.Type()

	// 4. 获取返回值的数量
	numOut := fnType.NumOut()

	// 5. 如果函数有返回值，则返回第一个返回值的类型
	if numOut > 0 {
		return fnType.Out(0)
	}

	// 6. 如果函数没有返回值，则返回 nil
	return nil
}

func copyMap(src map[int][]byte) map[int][]byte {
	if src == nil {
		return nil
	}
	dst := make(map[int][]byte, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func mapOrderedIterate[V any](m map[string]V, f func(key string, value V)) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		f(k, m[k])
	}
}

// callableType represents a method type or function type.
type callableType struct {
	reflect.Type
	argOffset int
}

func newCallableType(typ reflect.Type, isMethod bool) *callableType {
	if typ.Kind() != reflect.Func {
		panic(fmt.Sprintf("newCallableType: type %v is not a function / method", typ))
	}
	argOffset := 0
	if isMethod {
		argOffset = 1
	}
	return &callableType{
		Type:      typ,
		argOffset: argOffset,
	}
}

func (m *callableType) IsValidArgNum(numIn int) bool {
	if m.Type.IsVariadic() {
		return numIn >= m.Type.NumIn()-m.argOffset-1
	}
	return numIn == m.Type.NumIn()-m.argOffset
}

func (m *callableType) InType(idx int) reflect.Type {
	if m.Type.IsVariadic() {
		// The index of the variadic argument (from the user's perspective) is NumIn() - 1 - m.argOffset.
		if idx >= m.Type.NumIn()-1-m.argOffset {
			// If the requested index is for the variadic part,
			// get the slice type of the last parameter...
			sliceType := m.Type.In(m.Type.NumIn() - 1)
			// ...and return its element type.
			return sliceType.Elem()
		}
	}
	return m.Type.In(idx + m.argOffset)
}
