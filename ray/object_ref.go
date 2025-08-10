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

// GetAllTimeout returns all return values of the ObjectRefs in []any.
//
// Parameter timeout set the maximum amount of time in seconds to wait before returning.
// Setting timeout=0 will return the object immediately if it’s available.
// Returns [ErrTimeout] if the object is not available within the specified timeout.
// Returns [ErrCancelled] if the task is cancelled.
func (obj ObjectRef) GetAllTimeout(timeout float64) ([]any, error) {
	resultData, err := obj.getRaw(timeout)
	if err != nil {
		return nil, err
	}
	res := decodeFuncResult(obj.originFunc, resultData)
	return res, nil
}

func (obj ObjectRef) numReturn() int {
	if obj.originFunc == nil {
		return 1 // ray.Put() ObjectRef
	}
	return obj.originFunc.NumOut()
}

// GetAll returns all return values of the ObjectRefs in []any.
// Returns [ErrCancelled] if the task is cancelled.
func (obj ObjectRef) GetAll() ([]any, error) {
	return obj.GetAllTimeout(-1)
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

// Get0 is used to wait remote task / actor method execution finish.
func (obj ObjectRef) Get0() error {
	_, err := obj.GetAll()
	return err
}

// Get1 is used to get the result of remote task / actor method with 1 return value.
func (obj ObjectRef) Get1() (any, error) {
	res, err := obj.GetAll()
	if err != nil {
		return nil, err
	}
	if len(res) != 1 {
		panic(fmt.Sprintf("ObjectRef.Get1: the number of return values error, expect 1 but got %v", len(res)))
	}
	return res[0], err
}

// Get2 is used to get the result of remote task / actor method with 2 return value.
func (obj ObjectRef) Get2() (any, any, error) {
	res, err := obj.GetAll()
	if err != nil {
		return nil, nil, err
	}
	if len(res) != 2 {
		panic(fmt.Sprintf("ObjectRef.Get2: the number of return values error, expect 2 but got %v", len(res)))
	}
	return res[0], res[1], err
}

// Get3 is used to get the result of remote task / actor method with 3 return value.
func (obj ObjectRef) Get3() (any, any, any, error) {
	res, err := obj.GetAll()
	if err != nil {
		return nil, nil, nil, err
	}
	if len(res) != 3 {
		panic(fmt.Sprintf("ObjectRef.Get3: the number of return values error, expect 3 but got %v", len(res)))
	}
	return res[0], res[1], res[2], err
}

// Get4 is used to get the result of remote task / actor method with 4 return value.
func (obj ObjectRef) Get4() (any, any, any, any, error) {
	res, err := obj.GetAll()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if len(res) != 4 {
		panic(fmt.Sprintf("ObjectRef.Get4: the number of return values error, expect 4 but got %v", len(res)))
	}
	return res[0], res[1], res[2], res[3], err
}

// Get5 is used to get the result of remote task / actor method with 5 return value.
func (obj ObjectRef) Get5() (any, any, any, any, any, error) {
	res, err := obj.GetAll()
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	if len(res) != 5 {
		panic(fmt.Sprintf("ObjectRef.Get5: the number of return values error, expect 5 but got %v", len(res)))
	}
	return res[0], res[1], res[2], res[3], res[4], err
}

// Get6 is used to get the result of remote task / actor method with 6 return value.
func (obj ObjectRef) Get6() (any, any, any, any, any, any, error) {
	res, err := obj.GetAll()
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	if len(res) != 6 {
		panic(fmt.Sprintf("ObjectRef.Get6: the number of return values error, expect 6 but got %v", len(res)))
	}
	return res[0], res[1], res[2], res[3], res[4], res[5], err
}
