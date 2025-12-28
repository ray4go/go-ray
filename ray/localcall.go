package ray

import (
	"encoding/binary"
	"fmt"
	"github.com/bytedance/gg/gslice"
	"github.com/ray4go/go-ray/ray/internal/consts"
	"github.com/ray4go/go-ray/ray/internal/ffi"
	"github.com/ray4go/go-ray/ray/internal/log"
	"github.com/ray4go/go-ray/ray/internal/remote_call"
	"github.com/ray4go/go-ray/ray/internal/utils"
	"reflect"
)

// LocalPyCallResult represents the result of a local Python call.
type LocalPyCallResult struct {
	data []byte
	code int64
}

// Get returns the result of the local Python call.
func (r LocalPyCallResult) Get() (any, error) {
	if r.code != consts.ErrorCode_Success {
		return nil, fmt.Errorf("local call Python failed: retCode=%v, message=%s", r.code, r.data)
	}
	res := remote_call.DecodeWithType(r.data, nil, utils.SliceIndexGetter([]reflect.Type{anyType}))
	if len(res) == 0 {
		return nil, nil
	} else {
		return res[0], nil
	}
}

// GetInto decodes the result into the provided pointers.
//
// If the Python function has no return values, no arguments should be provided.
// If the Python function has return value, only one pointer should be provided.
//
// For type conversion between Python and Go, see [GoRay Cross-Language Call Type Conversion Guide].
//
// [GoRay Cross-Language Call Type Conversion Guide]: https://github.com/ray4go/go-ray/blob/master/docs/crosslang_types.md
func (r LocalPyCallResult) GetInto(ptrs ...any) error {
	if r.code != consts.ErrorCode_Success {
		return fmt.Errorf("local call Python failed: retCode=%v, message=%s", r.code, r.data)
	}
	if len(ptrs) == 0 {
		return nil
	}
	if gslice.Any(ptrs, func(v any) bool { _, ok := v.(GetObjectOption); return ok }) {
		panic("GetObjectOption such as WithTimeout() is not supported for python local call task")
	}

	if len(ptrs) > 1 {
		return fmt.Errorf("GetInto error: the max number of pointer for local Python call is 1, got %v", len(ptrs))
	}
	return remote_call.DecodeInto(r.data, ptrs)
}

// LocalCallPyTask executes a Python task locally (in current process) by name with the provided arguments.
// Unlike [RemoteCall] and [ActorHandle.RemoteCall], this function is synchronous and blocks until the task completes
// and [ObjectRef] is not supported as arguments.
func LocalCallPyTask(name string, args ...any) LocalPyCallResult {
	log.Debug("[Go] LocalCallPyTask %s %#v\n", name, args)
	// todo: check no objref and RayOption in args
	argsAndOpts := append(args, Option(consts.GorayOptionKey_TaskName, name))
	argsData := remote_call.EncodeRemoteCallArgs(nil, remoteCallArgs(argsAndOpts))
	resData, retCode := ffi.CallServer(consts.Go2PyCmd_ExePythonLocalTask, argsData)
	return LocalPyCallResult{
		data: resData,
		code: retCode,
	}
}

// CallPythonCode executes Python code in current process.
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
	argsData := remote_call.EncodeRemoteCallArgs(nil, remoteCallArgs(argsAndOpts))
	resData, retCode := ffi.CallServer(consts.Go2PyCmd_ExePyCode, argsData)
	return LocalPyCallResult{
		data: resData,
		code: retCode,
	}
}

// PythonObjectHandle represents a handle to a Python object in the local process.
type PythonObjectHandle struct {
	pyLocalId int64 // python instance id
}

// NewLocalPyClassInstance initializes a new Python class instance locally (in current process).
//
// See [GoRay Cross-Language Programming] for more details.
//
// [GoRay Cross-Language Programming]: https://github.com/ray4go/go-ray/blob/master/docs/crosslang.md
func NewLocalPyClassInstance(className string, args ...any) *PythonObjectHandle {
	argsAndOpts := append(args, Option(consts.GorayOptionKey_ActorName, className))
	argsData := remote_call.EncodeRemoteCallArgs(nil, remoteCallArgs(argsAndOpts))

	res, retCode := ffi.CallServer(consts.Go2PyCmd_NewClassInstance, argsData)
	if retCode != 0 {
		panic(fmt.Sprintf("Error: NewActor failed: retCode=%v, message=%s", retCode, res))
	}
	return &PythonObjectHandle{
		pyLocalId: int64(binary.LittleEndian.Uint64(res)),
	}
}

// MethodCall calls a method on the local Python class instance.
func (h *PythonObjectHandle) MethodCall(methodName string, args ...any) LocalPyCallResult {
	argsAndOpts := append(args,
		Option(consts.GorayOptionKey_PyLocalActorId, h.pyLocalId),
		Option(consts.GorayOptionKey_TaskName, methodName),
	)
	argsData := remote_call.EncodeRemoteCallArgs(nil, remoteCallArgs(argsAndOpts))

	resData, retCode := ffi.CallServer(consts.Go2PyCmd_LocalMethodCall, argsData)
	return LocalPyCallResult{
		data: resData,
		code: retCode,
	}
}

// Close closes the local Python class instance.
func (h *PythonObjectHandle) Close() error {
	opts := []*RayOption{
		Option(consts.GorayOptionKey_PyLocalActorId, h.pyLocalId),
	}
	data, err := jsonEncodeOptions(opts)
	if err != nil {
		return fmt.Errorf("error to json encode ray RayOption: %w", err)
	}

	res, retCode := ffi.CallServer(consts.Go2PyCmd_CloseClassInstance, data)

	if retCode != consts.ErrorCode_Success {
		return fmt.Errorf("close python local class instance failed, reason: %w, detail: %s", newError(retCode), res)
	}
	return nil
}
