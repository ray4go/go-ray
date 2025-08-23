package ray

import (
	"github.com/ray4go/go-ray/ray/internal/consts"
	"github.com/ray4go/go-ray/ray/internal/ffi"
	"github.com/ray4go/go-ray/ray/internal/log"
	"fmt"
	"reflect"
	"strconv"
)

var (
	taskReceiverVal reflect.Value
	taskFuncs       map[string]reflect.Method
)

func registerTasks(taskReceiver any) {
	taskReceiverVal = reflect.ValueOf(taskReceiver)
	// TODO: check taskRcvr's underlying type is pointer of struct{} (make sure it's stateless)
	taskFuncsList := getExportedMethods(reflect.TypeOf(taskReceiver))
	taskFuncs = make(map[string]reflect.Method, len(taskFuncsList))
	for _, taskFunc := range taskFuncsList {
		taskFuncs[taskFunc.Name] = taskFunc
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

	taskFunc, ok := taskFuncs[name]
	if !ok {
		panic(fmt.Sprintf("Error: RemoteCall failed: task %s not found", name))
	}
	callable := newCallableType(taskFunc.Type, true)
	argsAndOpts = append(argsAndOpts, Option(consts.GorayOptionKey_TaskName, name))
	argsData := encodeRemoteCallArgs(callable, argsAndOpts)

	res, retCode := ffi.CallServer(consts.Go2PyCmd_ExeRemoteTask, argsData) // todo: pass error to ObjectRef
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

func handleRunTask(_ int64, data []byte) (resData []byte, retCode int64) {
	funcName, rawArgs, posArgs := unpackRemoteCallArgs(data)
	taskFunc, ok := taskFuncs[funcName]
	if !ok {
		panic(fmt.Sprintf("Error: RemoteCall failed: task %s not found", funcName))
	}
	args := decodeWithType(rawArgs, posArgs, newCallableType(taskFunc.Type, true).InType)
	res := funcCall(&taskReceiverVal, taskFunc.Func, args)
	resData = encodeFuncResult(res)
	log.Debug("funcCall %v -> %v\n", taskFunc, res)
	return resData, 0
}
