package ray

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"

	"github.com/ray4go/go-ray/ray/utils/log"
)

type MethodType struct {
	reflect.Type // must be a method type
}

func (m *MethodType) IsValidArgNum(numIn int) bool {
	if m.Type.IsVariadic() {
		return numIn >= m.Type.NumIn()-1
	}
	return numIn == m.Type.NumIn()-1
}

func (m *MethodType) InType(idx int) reflect.Type {
	if m.Type.IsVariadic() {
		// The index of the variadic argument (from the user's perspective) is NumIn() - 2.
		if idx >= m.Type.NumIn()-2 {
			// If the requested index is for the variadic part,
			// get the slice type of the last parameter...
			sliceType := m.Type.In(m.Type.NumIn() - 1)
			// ...and return its element type.
			return sliceType.Elem()
		}
	}
	return m.Type.In(idx + 1)
}

func encodeArgs(method reflect.Method, args []any, opsArgLen int) []byte {
	if !(&MethodType{method.Type}).IsValidArgNum(len(args) + opsArgLen) {
		panic(fmt.Sprintf(
			"encodeArgs: method `%s` args length not match, given %v, expect %v. MethodType: %s",
			method.Name, len(args)+opsArgLen, method.Type.NumIn()-1, method.Type,
		))
	}
	rawArgs := encodeSlice(args)
	return rawArgs
}

func decodeArgs(method reflect.Method, rawArgs []byte, posArgs map[int][]byte) []any {
	methodType := &MethodType{method.Type}
	return decodeWithType(rawArgs, posArgs, methodType.InType)
}

func funcCall(rcvrVal reflect.Value, method reflect.Method, rawArgs []byte, posArgs map[int][]byte) []byte {
	args := decodeArgs(method, rawArgs, posArgs)

	log.Debug("[Go] funcCall: %v", method.Name)

	funcVal := method.Func
	argVals := make([]reflect.Value, len(args)+1)
	argVals[0] = rcvrVal
	for i, arg := range args {
		argVals[i+1] = reflect.ValueOf(arg)
	}
	returnValues := funcVal.Call(argVals)

	results := make([]any, len(returnValues))
	for i, res := range returnValues {
		results[i] = res.Interface()
	}
	return encodeSlice(results)
}

func decodeFuncResult(method reflect.Method, rawResult []byte) []any {
	return decodeWithType(rawResult, nil, method.Type.Out)
}

func encodeSlice(items []any) []byte {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	for _, item := range items {
		err := enc.Encode(item)
		if err != nil {
			log.Panicf("gob encode type %v error: %v", reflect.TypeOf(item), err)
		}
	}
	return buffer.Bytes()
}

func decodeWithType(args []byte, posArgs map[int][]byte, typeGetter func(int) reflect.Type) []any {
	buf := bytes.NewBuffer(args)
	argsDec := gob.NewDecoder(buf)
	outs := make([]any, 0)
	posArgs = copyMap(posArgs)
	for idx := 0; buf.Len() > 0 || len(posArgs) > 0; idx++ {
		argData, ok := posArgs[idx]
		var dec *gob.Decoder
		if ok {
			dec = gob.NewDecoder(bytes.NewBuffer(argData))
			delete(posArgs, idx) // remove the processed pos arg
		} else {
			dec = argsDec
		}
		typ := typeGetter(idx)
		item := reflect.New(typ)
		err := dec.Decode(item.Interface())
		if err != nil {
			log.Printf("decodeWithTypes: %#v \n", item.Interface())
			log.Panicf("gob decode type `%v` error: %v", typ, err)
		}
		outs = append(outs, item.Elem().Interface())
	}
	return outs
}
