package ray

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
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

var (
	driverFunc    func()
	taskRcvrVal   reflect.Value
	taskRcvrTyp   reflect.Type
	taskFuncs     []reflect.Method
	tasksName2Idx map[string]int
)

// Init goray environment and register the ray driver and tasks.
// All public methods of the given taskRcvr will be registered as ray tasks.
// This function should be called in the init() function of your ray application.
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
		// MethodType must be exported.
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

/*
data format: multiple bytes units
bytes unit format: | length:8byte:int64 | data:${length}byte:[]byte |

- first unit is raw args data;
- other units are objectRefs resolved data;
  - resolved data format: | arg_pos:8byte:int64 | data:[]byte |
*/
func handleRunTask(taskIndex int64, data []byte) (res []byte, retCode int64) {
	taskFunc := taskFuncs[taskIndex]
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("funcCall panic: %v\n%s\n", err, debug.Stack())
			retCode = 1
			res = []byte(fmt.Sprintf("panic when call %s(): %v", taskFunc.Name, err))
		}
	}()

	args, ok := decodeBytesUnits(data)
	if !ok || len(args) == 0 {
		return []byte("Error: handleRunTask decode args failed"), 1
	}
	posArgs := make(map[int][]byte)
	for i := 1; i < len(args); i++ { // skip first unit, which is raw args
		pos := int(binary.LittleEndian.Uint64(args[i][:8]))
		posArgs[pos] = args[i][8:]
	}

	res = funcCall(taskRcvrVal, taskFunc, args[0], posArgs)
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

// TaskOption is used to pass options to RemoteCall().
type TaskOption struct{ *Option }

func WithTaskOption(name string, value any) *TaskOption {
	return &TaskOption{NewOption(name, value)}
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

func encodeOptions(opts []*TaskOption, objRefs map[int]ObjectRef) []byte {
	kvs := make(map[string]any)
	for _, opt := range opts {
		kvs[opt.Name()] = opt.Value()
	}
	objIdx2Ids := make(map[int]int64)
	for idx, obj := range objRefs {
		objIdx2Ids[idx] = obj.id
	}
	kvs["go_ray_object_pos_to_local_id"] = objIdx2Ids
	data, err := json.Marshal(kvs)
	if err != nil {
		panic(fmt.Sprintf("Error encoding options to JSON: %v", err))
	}
	return data
}

// RemoteCall calls the remote task of the given name with the given arguments.
// The ray task options can be passed in the last with WithTaskOption(key, value).
// The call is asynchronous, and returns an ObjectRef that can be used to get the result later.
// The ObjectRef can also be passed to a remote task or actor method as an argument.
func RemoteCall(name string, argsAndOpts ...any) ObjectRef {
	args, opts := splitArgsAndOptions(argsAndOpts)
	funcId, ok := tasksName2Idx[name]
	if !ok {
		panic(fmt.Sprintf("Error: RemoteCall failed: task %s not found", name))
	}
	log.Debug("[Go] RemoteCall %s %#v\n", name, args)
	taskFunc := taskFuncs[funcId]

	args, objRefs := splitsplitArgsAndObjectRefs(args)
	argData := encodeArgs(taskFunc, args, len(objRefs))
	optData := encodeOptions(opts, objRefs)
	data := append(argData, optData...) // TODO: optimize the memory allocation

	// request bitmap layout (64 bits, LSB first)
	// | cmdId   | taskIndex | optionLength |
	// | 10 bits | 22 bits   | 32 bits      |
	request := Go2PyCmd_ExeRemoteTask | int64(funcId)<<cmdBitsLen | int64(len(optData))<<32
	res, retCode := ffi.CallServer(request, data) // todo: pass error to ObjectRef
	if retCode != 0 {
		panic(fmt.Sprintf("Error: RemoteCall failed: retCode=%v, message=%s", retCode, res))
	}
	id, _ := strconv.ParseInt(string(res), 10, 64) // todo: pass error to ObjectRef
	return ObjectRef{
		id:        id,
		taskIndex: funcId,
	}
}

func splitsplitArgsAndObjectRefs(items []any) ([]any, map[int]ObjectRef) {
	args := make([]any, 0, len(items))
	objs := make(map[int]ObjectRef)
	for idx, item := range items {
		switch v := item.(type) {
		case ObjectRef:
			objs[idx] = v
		case *ObjectRef:
			if v == nil {
				panic("invalid ObjectRef, got nil")
			}
			objs[idx] = *v
		default:
			args = append(args, item)
		}
	}
	return args, objs
}

// Put stores an object in the object store.
// Noted the returned ObjectRef can only be passed to a remote task or actor method. It cannot be used for ObjectRef.GetXXX().
// todo: return *ObjectRef
func Put(data any) (ObjectRef, error) {
	log.Debug("[Go] Put %#v\n", data)
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(data)
	if err != nil {
		return ObjectRef{-1, -1}, fmt.Errorf("gob encode type %v error: %v", reflect.TypeOf(data), err)
	}
	res, retCode := ffi.CallServer(Go2PyCmd_PutObject, buffer.Bytes()) // todo: pass error to ObjectRef
	if retCode != 0 {
		return ObjectRef{-1, -1}, fmt.Errorf("error: ray.Put() failed: retCode=%v, message=%s", retCode, res)
	}
	id, _ := strconv.ParseInt(string(res), 10, 64) // todo: pass error to ObjectRef
	return ObjectRef{
		id:        id,
		taskIndex: -1,
	}, nil
}

// Cancel a remote function (Task) or a remote Actor method (Actor Task)
// See https://docs.ray.io/en/latest/ray-core/api/doc/ray.cancel.html#ray-cancel
func (obj ObjectRef) Cancel(opts ...*Option) error {
	kvs := EncodeOptions(opts)
	kvs["object_ref_local_id"] = obj.id
	data, err := json.Marshal(kvs)
	if err != nil {
		log.Panicf("Error encoding options to JSON: %v", err)
	}
	res, retCode := ffi.CallServer(Go2PyCmd_CancelObject, data)
	if retCode != ErrorCode_Success {
		return fmt.Errorf("ray.Cancel() failed, reason: %w, detail: %s", NewError(retCode), res)
	}
	return nil
}

// Wait return a list of IDs that are ready and a list of IDs that are not.
// See https://docs.ray.io/en/latest/ray-core/api/doc/ray.wait.html#ray.wait
func Wait(objRefs []ObjectRef, opts ...*Option) ([]ObjectRef, []ObjectRef, error) {
	objIds := make([]int64, 0, len(objRefs))
	for _, obj := range objRefs {
		objIds = append(objIds, obj.id)
	}
	kvs := EncodeOptions(opts)
	kvs["object_ref_local_ids"] = objIds
	data, err := json.Marshal(kvs)
	if err != nil {
		log.Panicf("Error encoding options to JSON: %v", err)
	}
	retData, retCode := ffi.CallServer(Go2PyCmd_WaitObject, data)
	if retCode != ErrorCode_Success {
		return nil, nil, fmt.Errorf("ray.Wait() failed, reason: %w, detail: %s", NewError(retCode), retData)
	}
	var res [][]int
	err = json.Unmarshal(retData, &res)
	if err != nil {
		log.Panicf("ray.Wait(): decode response failed, response: %s", retData)
	}
	ready := make([]ObjectRef, len(res[0]))
	notReady := make([]ObjectRef, len(res[1]))
	for i, idx := range res[0] {
		ready[i] = objRefs[idx]
	}
	for i, idx := range res[1] {
		notReady[i] = objRefs[idx]
	}
	return ready, notReady, nil
}

// CallPythonCode executes python code in current ray worker.
// You can use `write(str)` function to write result as the return value.
// The `write` function can be used multiple times.
func CallPythonCode(code string) (string, error) {
	log.Debug("[Go] RunPythonCode %s\n", code)
	data, retCode := ffi.CallServer(Go2PyCmd_ExePyCode, []byte(code))
	log.Debug("[Go] RunPythonCode res: %v\n", string(data))
	if retCode != 0 {
		return "", fmt.Errorf("RunPythonCode failed: retCode=%v, message=%s", retCode, data)
	}
	return string(data), nil
}
