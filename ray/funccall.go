package ray

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"

	"github.com/ray4go/go-ray/ray/utils/log"
)

func encodeArgs(callable *CallableType, args []any, opsArgLen int) []byte {
	if !(callable.IsValidArgNum(len(args) + opsArgLen)) {
		panic(fmt.Sprintf(
			"encodeArgs: func/method args length not match, given %v, expect %v. CallableType: %s",
			len(args)+opsArgLen, callable.NumIn()-1, callable.Type,
		))
	}
	rawArgs := encodeSlice(args)
	return rawArgs
}

func funcCall(rcvrVal *reflect.Value, funcVal reflect.Value, callable *CallableType, rawArgs []byte, posArgs map[int][]byte) []any {
	args := decodeWithType(rawArgs, posArgs, callable.InType)
	log.Debug("[Go] funcCall: %v", callable.Type)

	argVals := make([]reflect.Value, 0, len(args)+1)
	if rcvrVal != nil {
		argVals = append(argVals, *rcvrVal)
	}
	for _, arg := range args {
		argVals = append(argVals, reflect.ValueOf(arg))
	}
	returnValues := funcVal.Call(argVals)

	results := make([]any, len(returnValues))
	for i, res := range returnValues {
		results[i] = res.Interface()
	}
	return results
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
