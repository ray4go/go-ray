package main

import (
	"fmt"
	"log"

	"github.com/ray4go/go-ray/ffi"
)

var (
	driverFunc func()
	taskFuncs  []any
)

type ObjectRef string

const cmdBitsLen = 10
const cmdBitsMask = (1 << cmdBitsLen) - 1

const (
	Go2PyCmd_Init          = 0
	Go2PyCmd_ExeRemoteTask = iota
	Go2PyCmd_GetObjects    = iota
)

const (
	Py2GoCmd_StartDriver = 0
	Py2GoCmd_RunTask     = iota
)

func Init(driverFunc_ func(), taskFuncs_ ...any) {
	log.SetFlags(log.Lshortfile)
	fmt.Printf("[Go] Init %v %v\n", driverFunc_, taskFuncs_)
	driverFunc = driverFunc_
	taskFuncs = taskFuncs_
	// time.Sleep(4 * time.Second)
	ffi.RegisterHandler(handlePythonCmd)
}

func handleStartDriver(_ int64, data []byte) []byte {
	driverFunc()
	return []byte{}
}

func handleRunTask(taskIndex int64, data []byte) []byte {
	taskFunc := taskFuncs[taskIndex]
	return funcCall(taskFunc, data)
}

var py2GoCmdHandlers = map[int64]func(int64, []byte) []byte{
	Py2GoCmd_StartDriver: handleStartDriver,
	Py2GoCmd_RunTask:     handleRunTask,
}

func handlePythonCmd(cmd int64, data []byte) []byte {
	cmdId := cmd & cmdBitsMask
	index := cmd >> cmdBitsLen
	fmt.Printf("[Go] handlePythonCmd cmdId:%d, index:%d\n", cmdId, index)

	handler, ok := py2GoCmdHandlers[cmdId]
	if !ok {
		log.Fatalf("[Go] Error: handlePythonCmd invalid cmdId %v\n", cmdId)
	}
	return handler(index, data)
}

func RemoteCall(funcId int64, args ...any) ObjectRef {
	fmt.Printf("[Go] RemoteCall %v %#v\n", funcId, args)
	data := encodeArgs(args)
	cmd := Go2PyCmd_ExeRemoteTask | funcId<<10
	res := ffi.CallServer(cmd, data)
	return ObjectRef(res)
}

func Get(obj ObjectRef) any {
	fmt.Printf("[Go] Get ObjectRef(%#v)\n", obj)
	data := ffi.CallServer(Go2PyCmd_GetObjects, []byte(obj))
	res := decodeResult(data)
	return res[0]
}
