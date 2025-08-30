package ray

import (
	"github.com/ray4go/go-ray/ray/internal/consts"
	"github.com/ray4go/go-ray/ray/internal/ffi"
	"github.com/ray4go/go-ray/ray/internal/log"
	"github.com/ray4go/go-ray/ray/internal/remote_call"
	"github.com/ray4go/go-ray/ray/internal/utils"
	"encoding/binary"
	"fmt"
	"reflect"
)

var (
	taskReceiverVal reflect.Value
	taskFuncs       map[string]reflect.Method
)

func registerTasks(taskReceiver any) {
	taskReceiverVal = reflect.ValueOf(taskReceiver)
	// TODO: check taskRcvr's underlying type is pointer of struct{} (make sure it's stateless)
	taskFuncsList := utils.GetExportedMethods(reflect.TypeOf(taskReceiver), false)
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
	callable := utils.NewCallableType(taskFunc.Type, true)
	argsAndOpts = append(argsAndOpts, Option(consts.GorayOptionKey_TaskName, name))
	argsData := remote_call.EncodeRemoteCallArgs(callable, remoteCallArgs(argsAndOpts))

	res, retCode := ffi.CallServer(consts.Go2PyCmd_ExeRemoteTask, argsData) // todo: pass error to ObjectRef
	if retCode != 0 {
		panic(fmt.Sprintf("Error: RemoteCall failed: retCode=%v, message=%s", retCode, res))
	}
	return ObjectRef{
		id:         int64(binary.LittleEndian.Uint64(res)),
		originFunc: taskFunc.Type,
	}
}

func handleRunTask(_ int64, data []byte) (resData []byte, retCode int64) {
	funcName, rawArgs, posArgs := remote_call.UnpackRemoteCallArgs(data)
	taskFunc, ok := taskFuncs[funcName]
	if !ok {
		panic(fmt.Sprintf("Error: RemoteCall failed: task %s not found", funcName))
	}
	args := remote_call.DecodeWithType(rawArgs, posArgs, utils.NewCallableType(taskFunc.Type, true).InType)
	res := remote_call.FuncCall(&taskReceiverVal, taskFunc.Func, args)
	resData = remote_call.EncodeFuncResult(res)
	log.Debug("FuncCall %v -> %v\n", taskFunc, res)
	return resData, 0
}
