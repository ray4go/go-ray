package ray

import (
	"github.com/ray4go/go-ray/ray/ffi"
	"github.com/ray4go/go-ray/ray/internal"
	"github.com/ray4go/go-ray/ray/utils/log"
	"fmt"
	"reflect"
	"strconv"
)

var dummyPyFunc = reflect.TypeOf(func() any { return nil })

// RemoteCallPyTask executes a remote Python ray task by name with the provided arguments and options.
// Like in [RemoteCall], [ObjectRef] instances can be passed as arguments.
func RemoteCallPyTask(name string, argsAndOpts ...any) ObjectRef {
	log.Debug("[Go] RemoteCallPyTask %s %#v\n", name, argsAndOpts)
	argsAndOpts = append(argsAndOpts, Option("task_name", name))
	argsData := encodeRemoteCallArgs(nil, argsAndOpts)
	request := internal.Go2PyCmd_ExePythonRemoteTask
	res, retCode := ffi.CallServer(int64(request), argsData) // todo: pass error to ObjectRef
	if retCode != 0 {
		panic(fmt.Sprintf("Error: RemoteCallPyTask failed: retCode=%v, message=%s", retCode, res))
	}
	id, err := strconv.ParseInt(string(res), 10, 64) // todo: pass error to ObjectRef
	if err != nil {
		panic(fmt.Sprintf("Error: RemoteCall invald return: %s, expect a number", res))
	}

	return ObjectRef{
		id:         id,
		originFunc: dummyPyFunc,
	}
}

// LocalCallPyTask executes a Python task locally (in current process) by name with the provided arguments.
// Noted: [ObjectRef] is not supported as arguments.
func LocalCallPyTask(name string, args ...any) (any, error) {
	log.Debug("[Go] LocalCallPyTask %s %#v\n", name, args)
	// todo: check no objref and option in args
	argsAndOpts := append(args, Option("task_name", name))
	argsData := encodeRemoteCallArgs(nil, argsAndOpts)
	request := internal.Go2PyCmd_ExePythonLocalTask
	resData, retCode := ffi.CallServer(int64(request), argsData) // todo: pass error to ObjectRef
	if retCode != 0 {
		return nil, fmt.Errorf("Error: LocalCallPyTask failed: retCode=%v, message=%s", retCode, resData)
	}
	res := decodeFuncResult(dummyPyFunc, resData)
	return res[0], nil
}

// NewPyActor creates a remote Python actor instance of the given class name with the provided arguments.
// [ObjectRef] can be passed as arguments.
func NewPyActor(className string, argsAndOpts ...any) *ActorHandle {
	log.Debug("[Go] NewPyActor %s %#v\n", className, argsAndOpts)
	argsAndOpts = append(argsAndOpts, Option("actor_class_name", className))
	argsData := encodeRemoteCallArgs(nil, argsAndOpts)

	res, retCode := ffi.CallServer(internal.Go2PyCmd_NewPythonActor, argsData)
	if retCode != 0 {
		panic(fmt.Sprintf("Error: NewActor failed: retCode=%v, message=%s", retCode, res))
	}
	id, err := strconv.ParseInt(string(res), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Error: NewActor invald return: %s, expect a number", res))
	}
	return &ActorHandle{
		pyLocalId: id,
		typ:       nil,
	}
}
