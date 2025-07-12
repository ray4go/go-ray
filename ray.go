package main

import (
	"fmt"
	"go/token"
	"log"
	"reflect"

	"github.com/ray4go/go-ray/ffi"
)

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

type ObjectRef struct {
	id        string
	taskIndex int
}

var (
	driverFunc func()
	// taskFuncs  []any
	taskRcvrVal   reflect.Value
	taskRcvrTyp   reflect.Type
	taskFuncs     []reflect.Method
	tasksName2Idx map[string]int
)

func Init(driverFunc_ func(), taskRcvr any) {
	log.SetFlags(log.Lshortfile)

	driverFunc = driverFunc_
	taskRcvrVal = reflect.ValueOf(taskRcvr)
	taskRcvrTyp = reflect.TypeOf(taskRcvr)
	// TODO: check taskRcvr's underlying type is pointer of struct{} (make sure it's stateless)
	taskFuncs = getExportedMethods(taskRcvrTyp)
	tasksName2Idx = make(map[string]int)
	for i, task := range taskFuncs {
		tasksName2Idx[task.Name] = i
	}

	fmt.Printf("[Go] Init %v %v\n", driverFunc_, tasksName2Idx)
	ffi.RegisterHandler(handlePythonCmd)
}

func getExportedMethods(typ reflect.Type) []reflect.Method {
	methods := make([]reflect.Method, 0)
	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		mtype := method.Type
		// Method must be exported.
		ok := method.IsExported()
		// All arguments must be exported or builtin types.
		for j := 1; j < mtype.NumIn(); j++ {
			argType := mtype.In(j)
			if !isExportedOrBuiltinType(argType) {
				fmt.Printf("[Go] method %v arg %v is not exported\n", method, argType)
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
			fmt.Printf("[Go] skip method %v\n", method)
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

func handleStartDriver(_ int64, data []byte) []byte {
	driverFunc()
	return []byte{}
}

func handleRunTask(taskIndex int64, data []byte) []byte {
	taskFunc := taskFuncs[taskIndex]
	return funcCall(taskRcvrVal, taskFunc, data)
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

func RemoteCall(name string, args ...any) ObjectRef {
	fmt.Printf("[Go] RemoteCall %s %#v\n", name, args)
	funcId := tasksName2Idx[name]
	taskFunc := taskFuncs[funcId]
	data := encodeArgs(taskFunc, args)
	cmd := Go2PyCmd_ExeRemoteTask | int64(funcId)<<cmdBitsLen
	res := ffi.CallServer(cmd, data)
	return ObjectRef{
		id:        string(res),
		taskIndex: funcId,
	}
}

func Get(obj ObjectRef) any {
	fmt.Printf("[Go] Get ObjectRef(%#v)\n", obj.id)
	data := ffi.CallServer(Go2PyCmd_GetObjects, []byte(obj.id))
	taskFunc := taskFuncs[obj.taskIndex]
	res := decodeResult(taskFunc, data)
	return res[0]
}
