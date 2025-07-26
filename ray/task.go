package ray

import (
	"github.com/ray4go/go-ray/ray/ffi"
	"github.com/ray4go/go-ray/ray/utils/log"
	"fmt"
	"reflect"
	"strconv"
)

var (
	taskReceiverVal reflect.Value
	taskFuncs       []reflect.Method
	tasksName2Idx   map[string]int
)

func registerTasks(taskReceiver any) {
	taskReceiverVal = reflect.ValueOf(taskReceiver)
	// TODO: check taskRcvr's underlying type is pointer of struct{} (make sure it's stateless)
	taskFuncs = getExportedMethods(reflect.TypeOf(taskReceiver))
	tasksName2Idx = make(map[string]int)
	for i, task := range taskFuncs {
		tasksName2Idx[task.Name] = i
	}
}

// RemoteCall calls the remote task of the given name with the given arguments.
// The ray task options can be passed in the last with WithTaskOption(key, value).
// The call is asynchronous, and returns an ObjectRef that can be used to get the result later.
// The ObjectRef can also be passed to a remote task or actor method as an argument.
func RemoteCall(name string, argsAndOpts ...any) ObjectRef {
	log.Debug("[Go] RemoteCall %s %#v\n", name, argsAndOpts)

	funcId, ok := tasksName2Idx[name]
	if !ok {
		panic(fmt.Sprintf("Error: RemoteCall failed: task %s not found", name))
	}
	taskFunc := taskFuncs[funcId]
	callable := NewCallableType(taskFunc.Type, true)
	argsData := encodeRemoteCallArgs(callable, argsAndOpts)

	// request bitmap layout (64 bits, LSB first)
	// | cmdId   | taskIndex |
	// | 10 bits | 54 bits   |
	request := Go2PyCmd_ExeRemoteTask | int64(funcId)<<cmdBitsLen
	res, retCode := ffi.CallServer(request, argsData) // todo: pass error to ObjectRef
	if retCode != 0 {
		panic(fmt.Sprintf("Error: RemoteCall failed: retCode=%v, message=%s", retCode, res))
	}
	id, err := strconv.ParseInt(string(res), 10, 64) // todo: pass error to ObjectRef
	if err != nil {
		panic(fmt.Sprintf("Error: RemoteCall invald return: %s, expect a number", res))
	}

	return ObjectRef{
		id:         id,
		originFunc: taskFunc.Type,
	}
}

func handleRunTask(taskIndex int64, data []byte) (resData []byte, retCode int64) {
	taskFunc := taskFuncs[taskIndex]
	args := decodeRemoteCallArgs(NewCallableType(taskFunc.Type, true), data)
	res := funcCall(&taskReceiverVal, taskFunc.Func, args)
	resData = encodeFuncResult(res)
	log.Debug("funcCall %v -> %v\n", taskFunc, res)
	return resData, 0
}
