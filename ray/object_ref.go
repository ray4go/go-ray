package ray

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/ray4go/go-ray/ray/internal/consts"
	"github.com/ray4go/go-ray/ray/internal/ffi"
	"github.com/ray4go/go-ray/ray/internal/log"
	"github.com/ray4go/go-ray/ray/internal/remote_call"
	"github.com/ray4go/go-ray/ray/internal/utils"
)

// ObjectRef is a reference to an object in Ray's object store.
// It serves as a future for the result of a remote task execution, an actor method call, or [ray.Put].
//   - Use ray.GetN series functions (such as [Get0], [Get1], ...), [ObjectRef.GetAll], or [ObjectRef.GetInto] to retrieve the result.
//   - Can be passed as an argument to remote Ray tasks (via [RemoteCall]) and actors (via [NewActor], [NewPyActor], [ActorHandle.RemoteCall]).
type ObjectRef struct {
	id          int64
	types       []reflect.Type // the types of the values referred by this ObjectRef. when the obj is from pytask, types will be []anyType
	autoRelease bool           // if true, the object will be automatically released after it is retrieved or passed to another task
}

type getObjectOption struct {
	timeout float64
}

// GetObjectOption defines options for ray.GetN, [ObjectRef.GetAll] and [ObjectRef.GetInto] functions.
//   - [WithTimeout] sets the timeout for getting the object.
type GetObjectOption func(*getObjectOption)

// WithTimeout sets the timeout for getting the object.
func WithTimeout(timeout time.Duration) GetObjectOption {
	return func(o *getObjectOption) {
		o.timeout = timeout.Seconds()
	}
}

func applyGetObjectOptions(opts []GetObjectOption) *getObjectOption {
	option := &getObjectOption{
		timeout: -1, // default: wait indefinitely
	}
	for _, opt := range opts {
		opt(option)
	}
	return option
}

// getRaw is used to get the raw result in bytes of the ObjectRef.
// timeout: -1 means wait indefinitely, 0 means return immediately if the object is available.
func (obj *ObjectRef) getRaw(option *getObjectOption) ([]byte, error) {
	data, err := json.Marshal([]any{obj.id, option.timeout, obj.autoRelease})
	if err != nil {
		return nil, fmt.Errorf("ObjectRef.Get json.Marshal failed: %w", err)
	}
	resultData, retCode := ffi.CallServer(consts.Go2PyCmd_GetObject, data)

	if retCode != consts.ErrorCode_Success {
		return nil, newError(retCode, resultData)
	}
	return resultData, nil
}

// NumReturn returns the number of return values of the remote task / actor method.
func (obj *ObjectRef) numReturn() int {
	return len(obj.types)
}

// By default, an object reference is automatically released once it has been retrieved or passed to another task.
// Subsequent attempts to use a released ObjectRef will trigger [ErrObjectRefNotFound].
// Use [ObjectRef.DisableAutoRelease] to turn off this behavior, the object reference will persist until task ends,
// or you can call [ObjectRef.Release] manually when you no longer need the reference in the current task.
// When all references to the object are released, the object will be garbage collected in the Ray object store.
func (obj *ObjectRef) DisableAutoRelease() {
	obj.autoRelease = false
}

// Release releases the current reference of the object.
// The object will be garbage collected if there are no other references to it.
//
// See [ObjectRef.DisableAutoRelease] for more details.
func (obj *ObjectRef) Release() {
	objIdData := make([]byte, 8)
	binary.LittleEndian.PutUint64(objIdData, uint64(obj.id))
	resultData, retCode := ffi.CallServer(consts.Go2PyCmd_ReleaseObject, objIdData)
	if retCode != consts.ErrorCode_Success && retCode != consts.ErrorCode_ObjectRefNotFound {
		panic(newError(retCode, resultData))
	}
}

// Cancel a remote function (Task) or a remote Actor method (Actor Task).
//
// Noted, for an actor method task, if the specified task is pending execution, it is canceled and not executed.
// If the actor method task is currently executing, the task cannot be canceled because actors have states.
// See [Ray Core API doc] for more info.
//
// [Ray Core API doc]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.cancel.html#ray-cancel
func (obj *ObjectRef) Cancel(opts ...*RayOption) error {
	data, err := jsonEncodeOptions(opts, Option("object_ref_local_id", obj.id))
	if err != nil {
		log.Panicf("Error encoding options to JSON: %v", err)
	}
	res, retCode := ffi.CallServer(consts.Go2PyCmd_CancelObject, data)
	if retCode != consts.ErrorCode_Success {
		return newError(retCode, res)
	}
	return nil
}

// GetAll returns all return values of the ObjectRefs as []any.
//
// [WithTimeout]() option sets the maximum amount of time to wait before returning.
// Setting timeout=0 returns immediately if the object is available.
//
// Returns [ErrTimeout] if the object is not available within the specified timeout.
// Returns [ErrCancelled] if the task is canceled.
func (obj *ObjectRef) GetAll(options ...GetObjectOption) ([]any, error) {
	resultData, err := obj.getRaw(applyGetObjectOptions(options))
	if err != nil {
		return nil, err
	}
	res := remote_call.DecodeWithType(resultData, nil, utils.SliceIndexGetter(obj.types))
	return res, nil
}

// GetInto is used to decode the result of remote task or actor method into the given pointer.
// The number of pointers must match the number of return values.
// If there are no return values, no pointers should be provided.
// Pass [WithTimeout]() as the last argument to set the timeout.
func (obj *ObjectRef) GetInto(ptrsAndOpts ...any) error {
	ptrs := make([]any, 0, len(ptrsAndOpts))
	opts := make([]GetObjectOption, 0, len(ptrsAndOpts))
	for _, val := range ptrsAndOpts {
		if opt, ok := val.(GetObjectOption); ok {
			opts = append(opts, opt)
		} else {
			ptrs = append(ptrs, val)
		}
	}

	resultData, err := obj.getRaw(applyGetObjectOptions(opts))
	if err != nil {
		return err
	}
	if len(ptrs) == 0 {
		return nil
	}
	return remote_call.DecodeInto(resultData, ptrs)
}

// [generic.Future1] & [SharedObject] implements this interface
type objectRefGetter interface {
	ObjectRef() *ObjectRef
}

// remoteCallArgs prepare arguments for [remote_call.EncodeRemoteCallArgs]
func remoteCallArgs(args []any) []any {
	checkObjectRef := func(obj *ObjectRef, argIdx int) {
		if obj.numReturn() != 1 {
			panic(fmt.Sprintf(
				"Error: invalid ObjectRef in arguments[%d], only accept ObjectRef with one return value."+
					"the ObjectRef you provided has %d return value", argIdx, obj.numReturn()))
		}
	}

	res := make([]any, len(args))
	for i, args := range args {
		switch v := args.(type) {
		case *ObjectRef:
			checkObjectRef(v, i)
			res[i] = &remote_call.RemoteObjectRef{Id: v.id, AutoRelease: v.autoRelease}
		case objectRefGetter: // generic.Future1 & SharedObject
			obj := v.ObjectRef()
			checkObjectRef(obj, i)
			res[i] = &remote_call.RemoteObjectRef{Id: obj.id, AutoRelease: obj.autoRelease}
		case *RayOption:
			res[i] = &remote_call.RemoteCallOption{Name: v.name, Value: v.value}
		default:
			res[i] = v
		}
	}
	return res
}
