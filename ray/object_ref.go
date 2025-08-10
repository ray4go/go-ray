package ray

import (
	"github.com/ray4go/go-ray/ray/ffi"
	"github.com/ray4go/go-ray/ray/internal"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// ObjectRef is a reference to an object in Ray's object store.
// It serves as a future for the result of a remote task execution, an actor method call or [ray.Put]().
type ObjectRef struct {
	originFunc reflect.Type // used to decode result, nil for ray.Put() ObjectRef
	id         int64
}

// getRaw is used to get the raw result in bytes of the ObjectRef.
// timeout: -1 means wait indefinitely, 0 means return immediately if the object is available.
func (obj ObjectRef) getRaw(timeout float64) ([]byte, error) {
	if obj.originFunc == nil {
		return nil, errors.New("cannot call Get on an ObjectRef of ray.Put(), pass it to a remote task or actor method instead")
	}

	data, err := json.Marshal([]any{obj.id, timeout})
	if err != nil {
		return nil, fmt.Errorf("ObjectRef.Get json.Marshal failed: %w", err)
	}
	resultData, retCode := ffi.CallServer(internal.Go2PyCmd_GetObject, data)

	if retCode != internal.ErrorCode_Success {
		return nil, fmt.Errorf("ObjectRef.Get failed, reason: %w, detail: %s", newError(retCode), resultData)
	}
	return resultData, nil
}

func (obj ObjectRef) numReturn() int {
	if obj.originFunc == nil {
		return 1 // ray.Put() ObjectRef
	}
	return obj.originFunc.NumOut()
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
	res := decodeFuncResult(obj.originFunc, resultData)
	return res, nil
}

// GetInto is used to decode the result of remote task / actor method into the given pointer.
// The number of pointers must match the number of return values of the remote task / actor method.
// If the remote task / actor method has no return values, no pointers should be provided.
// Pass a float to the last argument to set the timeout in seconds.
func (obj ObjectRef) GetInto(ptrs ...any) error {
	timeout := -1.0
	switch val := ptrs[len(ptrs)-1].(type) {
	case float32:
		timeout = float64(val)
		ptrs = ptrs[:len(ptrs)-1]
	case float64:
		timeout = val
		ptrs = ptrs[:len(ptrs)-1]
	}

	resultData, err := obj.getRaw(timeout)
	if err != nil {
		return err
	}
	if len(ptrs) == 0 {
		return nil
	}
	return decodeInto(resultData, ptrs)
}

// get the result of ObjectRef with n return value, with optional timeout.
func getN(obj ObjectRef, n int, timeout ...float64) ([]any, error) {
	timeoutVal := float64(-1)
	if len(timeout) > 0 {
		if len(timeout) != 1 {
			panic(fmt.Sprintf("ObjectRef Get%d: at most 1 timeout value is allowed, got %v", n, len(timeout)))
		}
		timeoutVal = timeout[0]
	}
	res, err := obj.GetAll(timeoutVal)
	if err == nil && len(res) < n {
		panic(fmt.Sprintf("ObjectRef Get%d: the number of return values mismatch, expect %d but got %v", n, n, len(res)))
	}
	return res, err
}
