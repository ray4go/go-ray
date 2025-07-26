package ray

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime/debug"
	"strconv"

	"github.com/ray4go/go-ray/ray/ffi"
	"github.com/ray4go/go-ray/ray/utils/log"
)

var (
	driverFunc func()
)

var py2GoCmdHandlers = map[int64]func(int64, []byte) ([]byte, int64){
	Py2GoCmd_StartDriver:     handleStartDriver,
	Py2GoCmd_RunTask:         handleRunTask,
	Py2GoCmd_NewActor:        handleCreateActor,
	Py2GoCmd_ActorMethodCall: handleActorMethodCall,
}

// Init goray environment and register the ray driver and tasks.
// All public methods of the given taskRcvr will be registered as ray tasks.
// This function should be called in the init() function of your ray application.
func Init(driverFunc_ func(), taskRcvr any, actorFactories map[string]any) {
	driverFunc = driverFunc_

	registerTasks(taskRcvr)
	registerActors(actorFactories)
	log.Debug("[Go] Init %v %v\n", driverFunc_, tasksName2Idx)
	ffi.RegisterHandler(handlePythonCmd)
}

func handlePythonCmd(request int64, data []byte) (resData []byte, retCode int64) {
	defer func() {
		if err := recover(); err != nil {
			retCode = ErrorCode_Failed
			resData = []byte(fmt.Sprintf("handlePythonCmd panic: %v\n%s\n", err, debug.Stack()))
		}
	}()

	cmdId := request & cmdBitsMask
	index := request >> cmdBitsLen
	log.Debug("[Go] handlePythonCmd cmdId:%d, index:%d\n", cmdId, index)

	handler, ok := py2GoCmdHandlers[cmdId]
	if !ok {
		return []byte(fmt.Sprintf("[Go] Error: handlePythonCmd invalid cmdId %v\n", cmdId)), 1
	}
	return handler(index, data)
}

func handleStartDriver(_ int64, _ []byte) ([]byte, int64) {
	driverFunc()
	return []byte{}, 0
}

// Put stores an object in the object store.
// Noted the returned ObjectRef can only be passed to a remote task or actor method. It cannot be used for ObjectRef.GetXXX().
// todo: return *ObjectRef
func Put(data any) (ObjectRef, error) {
	log.Debug("[Go] Put %#v\n", data)
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(data)
	if err != nil {
		return ObjectRef{nil, -1}, fmt.Errorf("gob encode type %v error: %v", reflect.TypeOf(data), err)
	}
	res, retCode := ffi.CallServer(Go2PyCmd_PutObject, buffer.Bytes()) // todo: pass error to ObjectRef
	if retCode != 0 {
		return ObjectRef{nil, -1}, fmt.Errorf("error: ray.Put() failed: retCode=%v, message=%s", retCode, res)
	}
	id, _ := strconv.ParseInt(string(res), 10, 64) // todo: pass error to ObjectRef
	return ObjectRef{
		id:         id,
		originFunc: nil,
	}, nil
}

// Cancel a remote function (Task) or a remote Actor method (Actor Task)
// See https://docs.ray.io/en/latest/ray-core/api/doc/ray.cancel.html#ray-cancel
func (obj ObjectRef) Cancel(opts ...*option) error {
	data, err := JsonEncodeOptions(opts, Option("object_ref_local_id", obj.id))
	if err != nil {
		log.Panicf("Error encoding options to JSON: %v", err)
	}
	res, retCode := ffi.CallServer(Go2PyCmd_CancelObject, data)
	if retCode != ErrorCode_Success {
		return fmt.Errorf("ray.Cancel() failed, reason: %w, detail: %s", NewError(retCode), res)
	}
	return nil
}

// Wait return a list of IDs that are ready and a list of IDs that are not.
// See https://docs.ray.io/en/latest/ray-core/api/doc/ray.wait.html#ray.wait
func Wait(objRefs []ObjectRef, opts ...*option) ([]ObjectRef, []ObjectRef, error) {
	objIds := make([]int64, 0, len(objRefs))
	for _, obj := range objRefs {
		objIds = append(objIds, obj.id)
	}
	data, err := JsonEncodeOptions(opts, Option("object_ref_local_ids", objIds))
	if err != nil {
		log.Panicf("Error encoding options to JSON: %v", err)
	}
	retData, retCode := ffi.CallServer(Go2PyCmd_WaitObject, data)
	if retCode != ErrorCode_Success {
		return nil, nil, fmt.Errorf("ray.Wait() failed, reason: %w, detail: %s", NewError(retCode), retData)
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
// You can use `write(str)` function to write result as the return value.
// The `write` function can be used multiple times.
func CallPythonCode(code string) (string, error) {
	log.Debug("[Go] RunPythonCode %s\n", code)
	data, retCode := ffi.CallServer(Go2PyCmd_ExePyCode, []byte(code))
	log.Debug("[Go] RunPythonCode res: %v\n", string(data))
	if retCode != 0 {
		return "", fmt.Errorf("RunPythonCode failed: retCode=%v, message=%s", retCode, data)
	}
	return string(data), nil
}
