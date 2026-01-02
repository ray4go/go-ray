/*
GoRay provides Golang bindings for the [Ray Core API] within the Ray distributed framework.

[User Guide]

[User Guide]: https://github.com/ray4go/go-ray#user-content-goray
[Ray Core API]: https://docs.ray.io/en/latest/ray-core/walkthrough.html
*/
package ray

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/bytedance/gg/gson"
	"reflect"

	"runtime/debug"

	"github.com/ray4go/go-ray/ray/internal/consts"
	"github.com/ray4go/go-ray/ray/internal/ffi"
	"github.com/ray4go/go-ray/ray/internal/log"
	"github.com/ray4go/go-ray/ray/internal/remote_call"
	"github.com/ray4go/go-ray/ray/internal/testing"
	"github.com/ray4go/go-ray/ray/internal/utils"
)

var (
	driverFunction func() int
	rayInitOptions []*RayOption
)

var py2GoCmdHandlers = map[int64]func(int64, []byte) ([]byte, int64){
	consts.Py2GoCmd_GetInitOptions:   handleGetInitOptions,
	consts.Py2GoCmd_StartDriver:      handleStartDriver,
	consts.Py2GoCmd_GetTaskActorList: handleGetTaskAndActorList,
	consts.Py2GoCmd_GetActorMethods:  handleGetActorMethods,
	consts.Py2GoCmd_RunTask:          handleRunTask,
	consts.Py2GoCmd_NewActor:         handleCreateActor,
	consts.Py2GoCmd_ActorMethodCall:  handleActorMethodCall,
	consts.Py2GoCmd_CloseActor:       handleCloseActor,
}

// Init the GoRay environment and register the ray tasks, actors and driver.
//   - All exported methods on taskRegister are registered as Ray tasks and can be invoked via [RemoteCall].
//   - Exported methods on actorRegister are used to create actors (via [NewActor]); each method serves as the actor’s constructor.
//     The method must return exactly one value (the actor instance), and should panic or return nil on failure to create the actor.
//   - driverFunc is called when the ray application starts and should return an integer as exit code.
//   - options are used to set the [ray.init()] options. In most cases, it's enough to pass no options.
//
// Call this function from the main package's init() function in your Ray application.
// All other GoRay APIs MUST be called within the driver function and its spawned remote actors/tasks.
//
// [ray.init()]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html#ray.init
func Init(taskRegister any, actorRegister any, driverFunc func() int, options ...*RayOption) {
	if driverFunction != nil {
		panic("Error: ray.Init() must be called only once")
	}
	driverFunction = driverFunc

	if taskRegister != nil {
		registerTasks(taskRegister)
	}
	if actorRegister != nil {
		registerActors(actorRegister)
	}
	rayInitOptions = options
	ffi.RegisterHandler(handlePythonCmd)
	go utils.ExitWhenCtrlC()
}

func handlePythonCmd(request int64, data []byte) (resData []byte, retCode int64) {
	defer func() {
		if panicInfo := recover(); panicInfo != nil {
			retCode = consts.ErrorCode_Panic
			// todo: skip top levels frame from stack trace
			resData, _ = gson.Marshal(panicError{Message: fmt.Sprint(panicInfo), Traceback: string(debug.Stack())})
		}
		testing.WriteCoverageWhenTesting()
	}()

	cmdId := request & consts.CmdBitsMask
	index := request >> consts.CmdBitsLen
	log.Debug("[Go] handlePythonCmd cmdId:%d, index:%d\n", cmdId, index)

	handler, ok := py2GoCmdHandlers[cmdId]
	if !ok {
		return []byte(fmt.Sprintf("[Go] Error: handlePythonCmd invalid cmdId %v\n", cmdId)), 1
	}
	return handler(index, data)
}

func handleGetInitOptions(_ int64, _ []byte) ([]byte, int64) {
	data, err := jsonEncodeOptions(rayInitOptions)
	if err != nil {
		return []byte(fmt.Sprintf("json encode ray  failed: %v", err)), consts.ErrorCode_Failed
	}
	return data, consts.ErrorCode_Success
}

func handleStartDriver(_ int64, _ []byte) ([]byte, int64) {
	if driverFunction == nil {
		return []byte("Error: driver function not set"), consts.ErrorCode_Failed
	}
	ret := driverFunction()
	return []byte(fmt.Sprint(ret)), consts.ErrorCode_Success
}

func handleGetTaskAndActorList(_ int64, _ []byte) ([]byte, int64) {
	taskNames := make([]string, 0, len(taskFuncs))
	for name := range taskFuncs {
		taskNames = append(taskNames, name)
	}
	actorNames := make([]string, 0, len(actorTypes))
	for name := range actorTypes {
		actorNames = append(actorNames, name)
	}
	data, err := json.Marshal([][]string{taskNames, actorNames})
	if err != nil {
		return []byte(fmt.Sprintf("Error: handleGetTaskAndActorList json.Marshal failed: %v", err)), consts.ErrorCode_Failed
	}
	return data, 0
}

// SharedObject represents a shared object put to ray object store by [Put]
type SharedObject[T0 any] struct {
	obj *ObjectRef // todo: embed *ObjectRef
}

// For internal use only. Do not call directly.
func (s SharedObject[T0]) ObjectRef() *ObjectRef {
	return s.obj
}

// Put stores an object in the object store.
// The returned value can be passed to a remote function (Task) or a remote actor method (Actor Task).
func Put[T any](value T) (SharedObject[T], error) {
	log.Debug("[Go] Put %#v\n", value)
	data, err := remote_call.EncodeValue(value)
	if err != nil {
		return SharedObject[T]{}, fmt.Errorf("encode %v error: %v", reflect.TypeOf(value), err)
	}
	objIdBytes, retCode := ffi.CallServer(consts.Go2PyCmd_PutObject, data) // todo: pass error to ObjectRef
	if retCode != consts.ErrorCode_Success {
		return SharedObject[T]{}, newError(retCode, objIdBytes)
	}

	var zero T
	obj := ObjectRef{
		id:          int64(binary.LittleEndian.Uint64(objIdBytes)),
		types:       []reflect.Type{reflect.TypeOf(zero)},
		autoRelease: false,
	}
	return SharedObject[T]{
		obj: &obj,
	}, nil
}

// Wait until the requested number of ObjectRefs are ready.
// Returns a list of ready ObjectRefs and a list of unready ObjectRefs.
//
// See [Ray Core API doc].
//
// [Ray Core API doc]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.wait.html#ray.wait
func Wait(objRefs []*ObjectRef, requestNum int, opts ...*RayOption) ([]*ObjectRef, []*ObjectRef, error) {
	objIds := make([]int64, 0, len(objRefs))
	for _, obj := range objRefs {
		objIds = append(objIds, obj.id)
	}
	data, err := jsonEncodeOptions(opts, Option("num_returns", requestNum), Option("object_ref_local_ids", objIds))
	if err != nil {
		log.Panicf("Error encoding options to JSON: %v", err)
	}
	retData, retCode := ffi.CallServer(consts.Go2PyCmd_WaitObject, data)
	if retCode != consts.ErrorCode_Success {
		return nil, nil, newError(retCode, retData)
	}
	var res [][]int
	err = json.Unmarshal(retData, &res)
	if err != nil {
		log.Panicf("ray.Wait(): decode response failed, response: %s", retData)
	}
	ready := make([]*ObjectRef, len(res[0]))
	notReady := make([]*ObjectRef, len(res[1]))
	for i, idx := range res[0] {
		ready[i] = objRefs[idx]
	}
	for i, idx := range res[1] {
		notReady[i] = objRefs[idx]
	}
	return ready, notReady, nil
}
