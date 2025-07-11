package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/ray4go/go-ray/ffi"
)

var (
	driverFunc func()
	taskFuncs  []any
)

type ObjectRef string

const cmdBitsLen = 10
const cmdBitsMask = (1 << cmdBitsLen) - 1

const (
	Go2PyCmd_Init          = 0
	Go2PyCmd_ExeRemoteTask = iota
	Go2PyCmd_GetObjects    = iota
)

const (
	Py2GoCmd_StartDriver = 0
	Py2GoCmd_RunTask     = iota
)

func Init(driverFunc_ func(), taskFuncs_ ...any) {
	log.SetFlags(log.Lshortfile)
	fmt.Printf("[Go] Init %v %v\n", driverFunc_, taskFuncs_)
	driverFunc = driverFunc_
	taskFuncs = taskFuncs_
	// time.Sleep(4 * time.Second)
	ffi.RegisterHandler(handlePythonCmd)
}

func handleStartDriver(_ int64, data []byte) []byte {
	driverFunc()
	return []byte{}
}

func handleRunTask(taskIndex int64, data []byte) []byte {
	taskFunc := taskFuncs[taskIndex]
	var args []any
	decode(data, &args)
	res := funcCall(taskFunc, args)
	return encode(res)
}

func funcCall(fun any, args []any) []any {
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
	return results
}

var py2GoCmdHandlers = map[int64]func(int64, []byte) []byte{
	Py2GoCmd_StartDriver: handleStartDriver,
	Py2GoCmd_RunTask:     handleRunTask,
}

func handlePythonCmd(cmd int64, data []byte) []byte {
	cmdId := cmd & cmdBitsMask
	index := cmd >> cmdBitsLen
	fmt.Printf("[Go] handlePythonCmd cmdId:%d, index:%d\n", cmdId, index)

	handler, ok := py2GoCmdHandlers[cmdId]
	if !ok {
		log.Fatalf("[Go] Error: handlePythonCmd invalid cmdId %v\n", cmdId)
	}
	return handler(index, data)
}

func RemoteCall(funcId int64, args ...any) ObjectRef {
	fmt.Printf("[Go] RemoteCall %v %#v\n", funcId, args)
	data := encode(args)
	cmd := Go2PyCmd_ExeRemoteTask | funcId<<10
	res := ffi.CallServer(cmd, data)
	return ObjectRef(res)
}

func GetObjects(objs []ObjectRef) []string {
	fmt.Printf("[Go] GetObjects %v\n", objs)

	strs := make([]string, len(objs))
	for i, v := range objs {
		strs[i] = string(v)
	}
	data := strings.Join(strs, ",")
	out := ffi.CallServer(Go2PyCmd_GetObjects, []byte(data))

	var res []string
	json.Unmarshal(out, &strs)
	return res
}

func Get(obj ObjectRef) any {
	fmt.Printf("[Go] Get ObjectRef(%#v)\n", obj)
	res := ffi.CallServer(Go2PyCmd_GetObjects, []byte(obj))
	var out []any
	decode(res, &out)
	return out[0]
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
