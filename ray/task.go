package ray

import (
	"github.com/ray4go/go-ray/ray/ffi"
	"github.com/ray4go/go-ray/ray/internal"
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

// RemoteCall calls the remote task by its name with the given arguments.
// The ray task options can be passed in the last with [Option](key, value).
// This call is asynchronous, returning an ObjectRef that resolves to the task's result.
// The returned ObjectRef can be used to retrieve the result or passed as an argument to other remote tasks or actor methods.
//
// For complete ray options, see [Ray Core API doc].
//
// [Ray Core API doc]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote_function.RemoteFunction.options.html#ray.remote_function.RemoteFunction.options
func RemoteCall(name string, argsAndOpts ...any) ObjectRef {
	log.Debug("[Go] RemoteCall %s %#v\n", name, argsAndOpts)

	funcId, ok := tasksName2Idx[name]
	if !ok {
		panic(fmt.Sprintf("Error: RemoteCall failed: task %s not found", name))
	}
	taskFunc := taskFuncs[funcId]
	callable := newCallableType(taskFunc.Type, true)
	argsAndOpts = append(argsAndOpts, Option("goray_task_name", name))
	argsData := encodeRemoteCallArgs(callable, argsAndOpts)

	// request bitmap layout (64 bits, LSB first)
	// | cmdId   | taskIndex |
	// | 10 bits | 54 bits   |
	request := internal.Go2PyCmd_ExeRemoteTask | int64(funcId)<<internal.CmdBitsLen
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
	args := decodeRemoteCallArgs(newCallableType(taskFunc.Type, true), data)
	res := funcCall(&taskReceiverVal, taskFunc.Func, args)
	resData = encodeFuncResult(res)
	log.Debug("funcCall %v -> %v\n", taskFunc, res)
	return resData, 0
}
