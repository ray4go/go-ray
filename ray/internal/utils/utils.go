package utils

import (
	"bytes"
	"github.com/ray4go/go-ray/ray/internal/log"
	"encoding/binary"
	"fmt"
	"go/token"
	"os"
	"os/signal"
	"reflect"
	"syscall"
)

// DecodeBytesUnits decodes byte units from a single bytes.
// Each unit has the format: | length:8byte:int64 | data:${length}byte:[]byte |
// It returns a slice of bytes, where each bytes is a data unit.
func DecodeBytesUnits(data []byte) ([][]byte, bool) {
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

func AppendBytesUnit(buffer *bytes.Buffer, data []byte) {
	// 1. Encode length prefix (8 bytes)
	length := make([]byte, 8)
	binary.LittleEndian.PutUint64(length, uint64(len(data)))
	buffer.Write(length)
	// 2. Append the actual data
	buffer.Write(data)
}

func GetExportedMethods(typ reflect.Type, allowReturnPrivateVal bool) []reflect.Method {
	methods := make([]reflect.Method, 0)
	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i) // deterministic: sort in lexicographic order.
		mtype := method.Type
		// CallableType must be exported.
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
		if !allowReturnPrivateVal {
			// all return values must be exported or builtin types.
			for j := 0; j < mtype.NumOut(); j++ {
				argType := mtype.Out(j)
				if !isExportedOrBuiltinType(argType) {
					ok = false
					break
				}
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

func ExitWhenCtrlC() {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT)
	go func() {
		<-c
		fmt.Println("Received SIGINT, exit...")
		os.Exit(0)
	}()
	// restore the original handling
	//signal.Reset(syscall.SIGINT)
}

// CallableType represents a method type or function type.
type CallableType struct {
	reflect.Type
	argOffset int // 1 for method, 0 for function
}

func NewCallableType(typ reflect.Type, isMethod bool) *CallableType {
	if typ.Kind() != reflect.Func {
		panic(fmt.Sprintf("NewCallableType: type %v is not a function / method", typ))
	}
	argOffset := 0
	if isMethod {
		argOffset = 1
	}
	return &CallableType{
		Type:      typ,
		argOffset: argOffset,
	}
}

// IsValidArgNum checks if the number of input arguments is valid.
func (m *CallableType) IsValidArgNum(numIn int) bool {
	if m.Type.IsVariadic() {
		return numIn >= m.Type.NumIn()-m.argOffset-1
	}
	return numIn == m.Type.NumIn()-m.argOffset
}

// InType returns the type of the i'th input argument. (0-based, not including receiver for methods)
func (m *CallableType) InType(idx int) reflect.Type {
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

func CopyMap(src map[int][]byte) map[int][]byte {
	if src == nil {
		return nil
	}
	dst := make(map[int][]byte, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// IsNilTypePointer checks if the given value is nil custom type pointer or interface.
func IsNilTypePointer(i any) bool {
	if i == nil {
		return true
	}
	value := reflect.ValueOf(i)
	kind := value.Kind()
	switch kind {
	case reflect.Ptr, reflect.Interface:
		return value.IsNil()
	default:
		return false
	}
}
