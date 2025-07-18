package ray

import (
	"encoding/json"
	"fmt"
	"go/token"
	"reflect"
	"runtime/debug"
	"strconv"

	"github.com/ray4go/go-ray/ray/ffi"
	"github.com/ray4go/go-ray/ray/utils/log"
)

const cmdBitsLen = 10
const cmdBitsMask = (1 << cmdBitsLen) - 1

const (
	Go2PyCmd_Init          = 0
	Go2PyCmd_ExeRemoteTask = iota
	Go2PyCmd_GetObjects    = iota
	Go2PyCmd_ExePyCode     = iota
)

const (
	Py2GoCmd_StartDriver = 0
	Py2GoCmd_RunTask     = iota
)

var (
	driverFunc    func()
	taskRcvrVal   reflect.Value
	taskRcvrTyp   reflect.Type
	taskFuncs     []reflect.Method
	tasksName2Idx map[string]int
)

func Init(driverFunc_ func(), taskRcvr any) {
	driverFunc = driverFunc_
	taskRcvrVal = reflect.ValueOf(taskRcvr)
	taskRcvrTyp = reflect.TypeOf(taskRcvr)
	// TODO: check taskRcvr's underlying type is pointer of struct{} (make sure it's stateless)
	taskFuncs = getExportedMethods(taskRcvrTyp)
	tasksName2Idx = make(map[string]int)
	for i, task := range taskFuncs {
		tasksName2Idx[task.Name] = i
	}

	log.Debug("[Go] Init %v %v\n", driverFunc_, tasksName2Idx)
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

func handleStartDriver(_ int64, _ []byte) (res []byte, retCode int64) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Driver panic: %v\n%s\n", err, debug.Stack())
			retCode = 1
			res = []byte(fmt.Sprintf("panic when run driver: %v", err))
		}
	}()
	driverFunc()
	return []byte{}, 0
}

func handleRunTask(taskIndex int64, data []byte) (res []byte, retCode int64) {
	taskFunc := taskFuncs[taskIndex]
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("funcCall panic: %v\n%s\n", err, debug.Stack())
			retCode = 1
			res = []byte(fmt.Sprintf("panic when call %s(): %v", taskFunc.Name, err))
		}
	}()
	res = funcCall(taskRcvrVal, taskFunc, data)
	log.Debug("funcCall %v -> %v\n", taskFunc, res)
	return res, 0
}

var py2GoCmdHandlers = map[int64]func(int64, []byte) ([]byte, int64){
	Py2GoCmd_StartDriver: handleStartDriver,
	Py2GoCmd_RunTask:     handleRunTask,
}

func handlePythonCmd(request int64, data []byte) ([]byte, int64) {
	cmdId := request & cmdBitsMask
	index := request >> cmdBitsLen
	log.Debug("[Go] handlePythonCmd cmdId:%d, index:%d\n", cmdId, index)

	handler, ok := py2GoCmdHandlers[cmdId]
	if !ok {
		return []byte(fmt.Sprintf("[Go] Error: handlePythonCmd invalid cmdId %v\n", cmdId)), 1
	}
	return handler(index, data)
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
		panic(fmt.Sprintf("Error encoding options to JSON: %v", err))
	}
	return data
}

func RemoteCall(name string, argsAndOpts ...any) ObjectRef {
	args, opts := splitArgsAndOptions(argsAndOpts)
	funcId := tasksName2Idx[name]
	taskFunc := taskFuncs[funcId]

	if taskFunc.Type.IsVariadic() {
		variadics := args[taskFunc.Type.NumIn()-2:] // pack variadic arguments into slice
		headArgs := args[:taskFunc.Type.NumIn()-2]
		args = make([]any, 0, taskFunc.Type.NumIn()-1)
		args = append(args, headArgs...)
		args = append(args, variadics)
	}
	return RemoteCallSlice(name, args, opts)
}

// if remote function is variadic, args[len(args)-1] should be function's final variadic arguments (slice)
func RemoteCallSlice(name string, args []any, opts []*TaskOption) ObjectRef {
	log.Debug("[Go] RemoteCall %s %#v\n", name, args)
	funcId := tasksName2Idx[name]
	taskFunc := taskFuncs[funcId]
	argData := encodeArgs(taskFunc, args)
	optData := encodeOptions(opts)
	data := append(argData, optData...) // TODO: optimize the memory allocation

	// request bitmap layout (64 bits, LSB first)
	// | cmdId   | taskIndex | optionLength |
	// | 10 bits | 22 bits   | 32 bits      |
	request := Go2PyCmd_ExeRemoteTask | int64(funcId)<<cmdBitsLen | int64(len(optData))<<32
	res, _ := ffi.CallServer(request, data)        // todo: pass error to ObjectRef
	id, _ := strconv.ParseInt(string(res), 10, 64) // todo: pass error to ObjectRef
	return ObjectRef{
		id:        id,
		taskIndex: funcId,
	}
}

func CallPythonCode(code string) (string, error) {
	log.Debug("[Go] RunPythonCode %s\n", code)
	data, retCode := ffi.CallServer(Go2PyCmd_ExePyCode, []byte(code))
	log.Debug("[Go] RunPythonCode res: %v\n", string(data))
	if retCode != 0 {
		return "", fmt.Errorf("RunPythonCode failed: retCode=%v, message=%s", retCode, data)
	}
	return string(data), nil
}
