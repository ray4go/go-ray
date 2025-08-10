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

// LocalPyCallResult represents the result of a local Python call.
type LocalPyCallResult struct {
	data []byte
	code int64
}

// Get returns the result of the local Python call.
func (r LocalPyCallResult) Get() (any, error) {
	if r.code != internal.ErrorCode_Success {
		return nil, fmt.Errorf("Error: Local Call Python failed: retCode=%v, message=%s", r.code, r.data)
	}
	res := decodeFuncResult(dummyPyFunc, r.data)
	if len(res) == 0 {
		return nil, nil
	} else {
		return res[0], nil
	}
}

// GetInto decodes the result into the provided pointers.
// The number of pointers must match the number of return values of the Python function.
// If the Python function has no return values, no arguments should be provided.
func (r LocalPyCallResult) GetInto(ptrs ...any) error {
	if r.code != internal.ErrorCode_Success {
		return fmt.Errorf("Error: Local Call Python failed: retCode=%v, message=%s", r.code, r.data)
	}
	if len(ptrs) == 0 {
		return nil
	}
	if len(ptrs) > 1 {
		return fmt.Errorf("GetInto error: the max number of pointer for local Python call is 1, got %v", len(ptrs))
	}
	return decodeInto(r.data, ptrs)
}

// LocalCallPyTask executes a Python task locally (in current process) by name with the provided arguments.
// Noted: [ObjectRef] is not supported as arguments.
func LocalCallPyTask(name string, args ...any) LocalPyCallResult {
	log.Debug("[Go] LocalCallPyTask %s %#v\n", name, args)
	// todo: check no objref and option in args
	argsAndOpts := append(args, Option("task_name", name))
	argsData := encodeRemoteCallArgs(nil, argsAndOpts)
	request := internal.Go2PyCmd_ExePythonLocalTask
	resData, retCode := ffi.CallServer(int64(request), argsData)
	return LocalPyCallResult{
		data: resData,
		code: retCode,
	}
}

// CallPythonCode executes python function code in current process.
// Example:
//
//	code := `
//	import math
//	def circle_perimeter(r):
//	    return 2 * math.pi * r
//	`
//	var perimeter float64
//	err := ray.CallPythonCode(code, 3.8).GetInto(&perimeter)
func CallPythonCode(funcCode string, args ...any) LocalPyCallResult {
	argsAndOpts := append(args, Option("func_code", funcCode))
	argsData := encodeRemoteCallArgs(nil, argsAndOpts)
	request := internal.Go2PyCmd_ExePyCode
	resData, retCode := ffi.CallServer(int64(request), argsData)
	return LocalPyCallResult{
		data: resData,
		code: retCode,
	}
}
