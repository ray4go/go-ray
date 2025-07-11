package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"reflect"
)

func encodeArgs(args []any) []byte {
	return encode(args)
}

func funcCall(fun any, rawArgs []byte) []byte {
	var args []any
	decode(rawArgs, &args)

	fmt.Println("[Go] funcCall:", fun, args)
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("funcCall panic:", err)
		}
	}()
	funcVal := reflect.ValueOf(fun)
	argVals := make([]reflect.Value, len(args))
	for i, arg := range args {
		argVals[i] = reflect.ValueOf(arg)
	}
	result := funcVal.Call(argVals)
	results := make([]any, len(result))
	for i, r := range result {
		results[i] = r.Interface()
	}
	return encode(results)
}

func decodeResult(result []byte) []any {
	var out []any
	decode(result, &out)
	return out
}

func encode(data any) []byte {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(data)
	if err != nil {
		log.Fatal("gob encode error:", err)
	}
	return buffer.Bytes()
}

func decode(data []byte, out any) {
	var buffer bytes.Buffer
	buffer.Write(data)
	dec := gob.NewDecoder(&buffer)
	err := dec.Decode(out)
	if err != nil {
		log.Fatal("gob decode error:", err)
	}
}
