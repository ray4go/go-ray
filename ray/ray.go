package ray

import (
	"github.com/ray4go/go-ray/ray/internal"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime/debug"
	"strconv"

	"github.com/ray4go/go-ray/ray/ffi"
	"github.com/ray4go/go-ray/ray/utils/log"
)

var (
	driverFunction func()
)

var py2GoCmdHandlers = map[int64]func(int64, []byte) ([]byte, int64){
	internal.Py2GoCmd_StartDriver:     handleStartDriver,
	internal.Py2GoCmd_GetInfo:         handleGetInfo,
	internal.Py2GoCmd_RunTask:         handleRunTask,
	internal.Py2GoCmd_NewActor:        handleCreateActor,
	internal.Py2GoCmd_ActorMethodCall: handleActorMethodCall,
}

// Init goray environment and register the ray driver and tasks.
// All public methods of the given taskReceiver will be registered as ray tasks.
// This function should be called in the init() function of your ray application.
func Init(driverFunc func(), taskReceiver any, actorFactories map[string]any) {
	driverFunction = driverFunc

	registerTasks(taskReceiver)
	registerActors(actorFactories)
	log.Debug("[Go] Init %v %v\n", driverFunc, tasksName2Idx)
	ffi.RegisterHandler(handlePythonCmd)
	go exitWhenCtrlC()
}

func handlePythonCmd(request int64, data []byte) (resData []byte, retCode int64) {
	defer func() {
		if err := recover(); err != nil {
			retCode = internal.ErrorCode_Failed
			resData = []byte(fmt.Sprintf("handlePythonCmd panic: %v\n%s\n", err, debug.Stack()))
		}
	}()

	cmdId := request & internal.CmdBitsMask
	index := request >> internal.CmdBitsLen
	log.Debug("[Go] handlePythonCmd cmdId:%d, index:%d\n", cmdId, index)

	handler, ok := py2GoCmdHandlers[cmdId]
	if !ok {
		return []byte(fmt.Sprintf("[Go] Error: handlePythonCmd invalid cmdId %v\n", cmdId)), 1
	}
	return handler(index, data)
}

func handleStartDriver(_ int64, _ []byte) ([]byte, int64) {
	driverFunction()
	return []byte{}, 0
}

func handleGetInfo(_ int64, _ []byte) ([]byte, int64) {
	data, err := json.Marshal([]any{tasksName2Idx, actorsName2Idx})
	if err != nil {
		return []byte(fmt.Sprintf("Error: handleGetInfo json.Marshal failed: %v", err)), internal.ErrorCode_Failed
	}
	return data, 0
}

// Put stores an object in the object store.
// Noted the returned ObjectRef can only be passed to a remote function (Task) or a remote Actor method (Actor Task).
// It cannot be used for ObjectRef.GetXXX().
func Put(value any) (ObjectRef, error) {
	log.Debug("[Go] Put %#v\n", value)
	data, err := encodeValue(value)
	if err != nil {
		return ObjectRef{nil, -1}, fmt.Errorf("gob encode type %v error: %v", reflect.TypeOf(value), err)
	}
	res, retCode := ffi.CallServer(internal.Go2PyCmd_PutObject, data) // todo: pass error to ObjectRef
	if retCode != 0 {
		return ObjectRef{nil, -1}, fmt.Errorf("error: ray.Put() failed: retCode=%v, message=%s", retCode, res)
	}
	id, _ := strconv.ParseInt(string(res), 10, 64) // todo: pass error to ObjectRef
	// todo: return *ObjectRef
	return ObjectRef{
		id:         id,
		originFunc: nil,
	}, nil
}

// Cancel a remote function (Task) or a remote Actor method (Actor Task)
// Noted, for actor method task, if the specified Task is pending execution, it is cancelled and not executed.
// If the actor method task is currently executing, the task cannot be canceled because actors have states.
// See https://docs.ray.io/en/latest/ray-core/api/doc/ray.cancel.html#ray-cancel
func (obj ObjectRef) Cancel(opts ...*option) error {
	data, err := jsonEncodeOptions(opts, Option("object_ref_local_id", obj.id))
	if err != nil {
		log.Panicf("Error encoding options to JSON: %v", err)
	}
	res, retCode := ffi.CallServer(internal.Go2PyCmd_CancelObject, data)
	if retCode != internal.ErrorCode_Success {
		return fmt.Errorf("ray.Cancel() failed, reason: %w, detail: %s", newError(retCode), res)
	}
	return nil
}

// Wait the requested number of ObjectRefs are ready.
// Returns a list of IDs that are ready and a list of IDs that are not.
// See https://docs.ray.io/en/latest/ray-core/api/doc/ray.wait.html#ray.wait
func Wait(objRefs []ObjectRef, requestNum int, opts ...*option) ([]ObjectRef, []ObjectRef, error) {
	objIds := make([]int64, 0, len(objRefs))
	for _, obj := range objRefs {
		objIds = append(objIds, obj.id)
	}
	data, err := jsonEncodeOptions(opts, Option("num_returns", requestNum), Option("object_ref_local_ids", objIds))
	if err != nil {
		log.Panicf("Error encoding options to JSON: %v", err)
	}
	retData, retCode := ffi.CallServer(internal.Go2PyCmd_WaitObject, data)
	if retCode != internal.ErrorCode_Success {
		return nil, nil, fmt.Errorf("ray.Wait() failed, reason: %w, detail: %s", newError(retCode), retData)
	}
	var res [][]int
	err = json.Unmarshal(retData, &res)
	if err != nil {
		log.Panicf("ray.Wait(): decode response failed, response: %s", retData)
	}
	ready := make([]ObjectRef, len(res[0]))
	notReady := make([]ObjectRef, len(res[1]))
	for i, idx := range res[0] {
		ready[i] = objRefs[idx]
	}
	for i, idx := range res[1] {
		notReady[i] = objRefs[idx]
	}
	return ready, notReady, nil
}

// CallPythonCode executes python code in current ray worker.
// You can use `write(str)` function in python to write result as the return value.
// The `write` function can be used multiple times.
func CallPythonCode(code string) (string, error) {
	log.Debug("[Go] RunPythonCode %s\n", code)
	data, retCode := ffi.CallServer(internal.Go2PyCmd_ExePyCode, []byte(code))
	log.Debug("[Go] RunPythonCode res: %v\n", string(data))
	if retCode != 0 {
		return "", fmt.Errorf("RunPythonCode failed: retCode=%v, message=%s", retCode, data)
	}
	return string(data), nil
}
