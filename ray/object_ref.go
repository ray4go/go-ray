package ray

import (
	"github.com/ray4go/go-ray/ray/internal/consts"
	"github.com/ray4go/go-ray/ray/internal/ffi"
	"github.com/ray4go/go-ray/ray/internal/remote_call"
	"github.com/ray4go/go-ray/ray/internal/utils"
	"encoding/json"
	"fmt"
	"reflect"
)

// ObjectRef is a reference to an object in Ray's object store.
// It serves as a future for the result of a remote task execution, an actor method call or [ray.Put]().
type ObjectRef struct {
	id    int64
	types []reflect.Type // the types of the object values
}

// getRaw is used to get the raw result in bytes of the ObjectRef.
// timeout: -1 means wait indefinitely, 0 means return immediately if the object is available.
func (obj ObjectRef) getRaw(timeout float64) ([]byte, error) {
	data, err := json.Marshal([]any{obj.id, timeout})
	if err != nil {
		return nil, fmt.Errorf("ObjectRef.Get json.Marshal failed: %w", err)
	}
	resultData, retCode := ffi.CallServer(consts.Go2PyCmd_GetObject, data)

	if retCode != consts.ErrorCode_Success {
		return nil, fmt.Errorf("ObjectRef.Get failed, reason: %w, detail: %s", newError(retCode), resultData)
	}
	return resultData, nil
}

// NumReturn returns the number of return values of the remote task / actor method.
func (obj ObjectRef) numReturn() int {
	return len(obj.types)
}

// GetAll returns all return values of the ObjectRefs in []any, with optional timeout.
// Returns [ErrCancelled] if the task is cancelled.
//
// Parameter timeout set the maximum amount of time in seconds to wait before returning.
// Setting timeout=0 will return the object immediately if it’s available.
// Returns [ErrTimeout] if the object is not available within the specified timeout.
// Returns [ErrCancelled] if the task is cancelled.
func (obj ObjectRef) GetAll(timeout ...float64) ([]any, error) {
	timeoutVal := float64(-1)
	if len(timeout) > 0 {
		if len(timeout) != 1 {
			panic(fmt.Sprintf("ObjectRef GetAll: at most 1 timeout value is allowed, got %v", len(timeout)))
		}
		timeoutVal = timeout[0]
	}

	resultData, err := obj.getRaw(timeoutVal)
	if err != nil {
		return nil, err
	}
	res := remote_call.DecodeWithType(resultData, nil, utils.SliceIndexGetter(obj.types))
	return res, nil
}

// GetInto is used to decode the result of remote task / actor method into the given pointer.
// The number of pointers must match the number of return values of the remote task / actor method.
// If the remote task / actor method has no return values, no pointers should be provided.
// Pass a float to the last argument to set the timeout in seconds.
func (obj ObjectRef) GetInto(ptrs ...any) error {
	timeout := -1.0
	if len(ptrs) > 0 {
		switch val := ptrs[len(ptrs)-1].(type) {
		case float32:
			timeout = float64(val)
			ptrs = ptrs[:len(ptrs)-1]
		case float64:
			timeout = val
			ptrs = ptrs[:len(ptrs)-1]
		}
	}

	resultData, err := obj.getRaw(timeout)
	if err != nil {
		return err
	}
	if len(ptrs) == 0 {
		return nil
	}
	return remote_call.DecodeInto(resultData, ptrs)
}

// [generic.Future1] & [Future] implements this interface
type objectRefGetter interface {
	ObjectRef() *ObjectRef
}

// remoteCallArgs prepare arguments for [remote_call.EncodeRemoteCallArgs]
func remoteCallArgs(args []any) []any {
	checkObjectRef := func(obj ObjectRef, argIdx int) {
		if obj.numReturn() != 1 {
			panic(fmt.Sprintf(
				"Error: invalid ObjectRef in arguments[%d], only accept ObjectRef with one return value."+
					"the ObjectRef you provided has %d return value", argIdx, obj.numReturn()))
		}
	}

	res := make([]any, len(args))
	for i, args := range args {
		switch v := args.(type) {
		case ObjectRef:
			checkObjectRef(v, i)
			res[i] = remote_call.RemoteObjectRefId(v.id)
		case objectRefGetter: // generic.Future1 & Future
			obj := v.ObjectRef()
			checkObjectRef(*obj, i)
			res[i] = remote_call.RemoteObjectRefId(obj.id)
		case *RayOption:
			res[i] = &remote_call.RemoteCallOption{Name: v.name, Value: v.value}
		default:
			res[i] = v
		}
	}
	return res
}
