/*
GoRay provides Golang support for the [Ray Core API] within the Ray distributed framework.

[User Guide]

[User Guide]: https://github.com/ray4go/go-ray#user-content-goray
[Ray Core API]: https://docs.ray.io/en/latest/ray-core/walkthrough.html
*/
package ray

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
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
)

var py2GoCmdHandlers = map[int64]func(int64, []byte) ([]byte, int64){
	consts.Py2GoCmd_StartDriver:      handleStartDriver,
	consts.Py2GoCmd_GetTaskActorList: handleGetTaskAndActorList,
	consts.Py2GoCmd_GetActorMethods:  handleGetActorMethods,
	consts.Py2GoCmd_RunTask:          handleRunTask,
	consts.Py2GoCmd_NewActor:         handleCreateActor,
	consts.Py2GoCmd_ActorMethodCall:  handleActorMethodCall,
	consts.Py2GoCmd_CloseActor:       handleCloseActor,
}

// Init goray environment and register the ray tasks, actors and driver.
//   - All public methods of the given taskReceiver will be registered as ray tasks.
//   - All public methods of the given actorFactories will be registered as actor constructor functions.
//     Each method name becomes the actor type name, and the method must return only one value (the actor instance).
//     The constructor method should panic or return nil when it fails to create the actor.
//   - driverFunc is the function to be called when the ray driver starts. The driverFunc should return an integer value as the exit code.
//
// This function should be called in the init() function of your ray application.
// All other goray APIs MUST be called after this function.
func Init(taskReceiver any, actorFactories any, driverFunc func() int) {
	driverFunction = driverFunc

	if taskReceiver != nil {
		registerTasks(taskReceiver)
	}
	if actorFactories != nil {
		registerActors(actorFactories)
	}
	ffi.RegisterHandler(handlePythonCmd)
	go utils.ExitWhenCtrlC()
}

func handlePythonCmd(request int64, data []byte) (resData []byte, retCode int64) {
	defer func() {
		if err := recover(); err != nil {
			retCode = consts.ErrorCode_Failed
			resData = []byte(fmt.Sprintf("handlePythonCmd panic: %v\n%s\n", err, debug.Stack()))
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

// This method is for internal use. Do not call it directly.
func (s SharedObject[T0]) ObjectRef() *ObjectRef {
	return s.obj
}

// Put stores an object in the object store.
// The returned value can be passed to a remote function (Task) or a remote Actor method (Actor Task).
func Put[T any](value T) (SharedObject[T], error) {
	log.Debug("[Go] Put %#v\n", value)
	data, err := remote_call.EncodeValue(value)
	if err != nil {
		return SharedObject[T]{}, fmt.Errorf("encode %v error: %v", reflect.TypeOf(value), err)
	}
	objIdBytes, retCode := ffi.CallServer(consts.Go2PyCmd_PutObject, data) // todo: pass error to ObjectRef
	if retCode != consts.ErrorCode_Success {
		return SharedObject[T]{}, fmt.Errorf("error: ray.Put() failed: retCode=%v, message=%s", retCode, objIdBytes)
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

// Wait the requested number of ObjectRefs are ready.
// Returns a list of IDs that are ready and a list of IDs that are not.
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
		return nil, nil, fmt.Errorf("ray.Wait() failed, reason: %w, detail: %s", newError(retCode), retData)
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
