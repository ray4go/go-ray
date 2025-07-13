package goray

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"reflect"
)

func encodeArgs(method reflect.Method, args []any) []byte {
	if len(args) != (method.Type.NumIn() - 1) {
		log.Panicf("encodeArgs [%#v]: args length not match, given %v, expect %v", method, len(args), method.Type.NumIn()-1)
	}
	data, err := encodeSlice(args)
	if err != nil {
		log.Panicf("encodeArgs [%#v]: encodeSlice error: %v", method, err)
	}
	return data
}

func decodeArgs(method reflect.Method, rawArgs []byte) []any {
	argTypes := make([]reflect.Type, 0, method.Type.NumIn()-1)
	for i := 1; i < method.Type.NumIn(); i++ {
		argTypes = append(argTypes, method.Type.In(i))
	}
	return decodeWithTypes(rawArgs, argTypes)
}

func funcCall(rcvrVal reflect.Value, method reflect.Method, rawArgs []byte) []byte {
	args := decodeArgs(method, rawArgs)
	fmt.Println("[Go] funcCall:", method.Name, args)

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
	data, err := encodeSlice(results)
	if err != nil {
		log.Panicf("encode return value (%#v) error: %v", results, err)
	}
	return data
}

func decodeResult(method reflect.Method, rawResult []byte) []any {
	retTypes := make([]reflect.Type, 0, method.Type.NumOut())
	for i := 0; i < method.Type.NumOut(); i++ {
		retTypes = append(retTypes, method.Type.Out(i))
	}
	return decodeWithTypes(rawResult, retTypes)
}

func encodeSlice(items []any) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	for _, item := range items {
		err := enc.Encode(item)
		if err != nil {
			return nil, fmt.Errorf("gob encode item %#v error: %v", item, err)
		}
	}
	return buffer.Bytes(), nil
}

func decodeWithTypes(data []byte, types []reflect.Type) []any {
	var buffer bytes.Buffer
	buffer.Write(data)
	dec := gob.NewDecoder(&buffer)
	outs := make([]any, 0, len(types))
	for _, typ := range types {
		item := reflect.New(typ)
		err := dec.Decode(item.Interface())
		if err != nil {
			log.Panicf("gob decode type %#v error: %v", typ.Name(), err)
		}
		outs = append(outs, item.Elem().Interface())
	}
	return outs
}
