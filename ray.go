package main

import (
	"encoding/json"
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
	taskIndex int
	pydata    []byte
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

func handlePythonCmd(cmd int64, data []byte) ([]byte, int64) {
	cmdId := cmd & cmdBitsMask
	index := cmd >> cmdBitsLen
	fmt.Printf("[Go] handlePythonCmd cmdId:%d, index:%d\n", cmdId, index)

	handler, ok := py2GoCmdHandlers[cmdId]
	if !ok {
		log.Fatalf("[Go] Error: handlePythonCmd invalid cmdId %v\n", cmdId)
	}
	return handler(index, data), 0
}

type TaskOption struct {
	name  string
	value any
}

func WithTaskOption(name string, value any) *TaskOption {
	return &TaskOption{
		name:  name,
		value: value,
	}
}

func splitArgsAndOptions(items []any) ([]any, []*TaskOption) {
	args := make([]any, 0, len(items))
	opts := make([]*TaskOption, 0, len(items))
	for _, item := range items {
		if opt, ok := item.(*TaskOption); ok {
			opts = append(opts, opt)
		} else {
			args = append(args, item)
		}
	}
	return args, opts
}

func encodeOptions(opts []*TaskOption) []byte {
	kvs := make(map[string]any)
	for _, opt := range opts {
		kvs[opt.name] = opt.value
	}
	data, err := json.Marshal(kvs)
	if err != nil {
		log.Panicf("Error encoding options to JSON: %v", err)
	}
	return data
}

func RemoteCall(name string, argsAndOpts ...any) ObjectRef {
	fmt.Printf("[Go] RemoteCall %s %#v\n", name, argsAndOpts)
	funcId := tasksName2Idx[name]
	taskFunc := taskFuncs[funcId]
	args, opts := splitArgsAndOptions(argsAndOpts)
	argData := encodeArgs(taskFunc, args)
	optData := encodeOptions(opts)
	data := append(argData, optData...) // TODO: optimize the memory allocation

	// request bitmap layout (64 bits, LSB first)
	// | cmdId   | taskIndex | optionLength |
	// | 10 bits | 22 bits   | 32 bits      |
	request := Go2PyCmd_ExeRemoteTask | int64(funcId)<<cmdBitsLen | int64(len(optData))<<32
	res, _ := ffi.CallServer(request, data)
	return ObjectRef{
		pydata:    res,
		taskIndex: funcId,
	}
}

// GetAll returns all return values of the given ObjectRefs.
func GetAll(obj ObjectRef) []any {
	fmt.Printf("[Go] Get ObjectRef(%v)\n", obj.taskIndex)
	data, _ := ffi.CallServer(Go2PyCmd_GetObjects, obj.pydata)
	taskFunc := taskFuncs[obj.taskIndex]
	res := decodeResult(taskFunc, data)
	return res
}

// Get returns the first return value of the given ObjectRefs.
func Get(obj ObjectRef) any {
	res := GetAll(obj)
	if len(res) == 0 {
		return nil
	}
	return res[0]
}

func Get2(obj ObjectRef) (any, any) {
	res := GetAll(obj)
	if len(res) < 2 {
		fmt.Println("[Go] Get2: len(res) < 2")
	}
	return res[0], res[1]
}
