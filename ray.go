package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

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
	var res []byte
	f, ok := taskFunc.(func([]byte) []byte)
	if ok {
		res = f(data)
	} else {
		fmt.Printf("[Go] Error: handlePythonCmd invalid taskFunc %v\n", taskFunc)
		panic("invalid taskFunc")
	}
	return res
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
		fmt.Printf("[Go] Error: handlePythonCmd invalid cmdId %v\n", cmdId)
		panic("invalid cmdId")
	}
	return handler(index, data)
}

func Remote(funcId int64, data []byte) ObjectRef {
	fmt.Printf("[Go] Remote %v\n", funcId)
	cmd := Go2PyCmd_ExeRemoteTask | funcId<<10
	res := ffi.CallServer(cmd, data)
	return ObjectRef(res)
}

func GetObjects(objs []ObjectRef) []string {
	fmt.Printf("[Go] GetObjects %v\n", objs)

	strs := make([]string, len(objs))
	for i, v := range objs {
		strs[i] = string(v)
	}
	data := strings.Join(strs, ",")
	out := ffi.CallServer(Go2PyCmd_GetObjects, []byte(data))

	var res []string
	json.Unmarshal(out, &strs)
	return res
}

func Get(obj ObjectRef) []byte {
	fmt.Printf("[Go] Get %v\n", obj)
	res := ffi.CallServer(Go2PyCmd_GetObjects, []byte(obj))
	return res
}

// -------------------------------------------------------
func init() {
	Init(driver, task1, task2)
}

func driver() {
	f1 := Remote(0, []byte("Hello Python!"))

	res := GetObjects([]ObjectRef{f1})
	fmt.Printf("[Go] f1: %s\n", res)
}

func task1(data []byte) []byte {
	fmt.Println("[Go] task1")
	time.Sleep(1 * time.Second)
	res := make([]byte, len(data))
	for i, v := range data {
		res[i] = (v + 1) % byte(255)
	}

	f2 := Remote(1, []byte("Hello Python!"))
	f2res := GetObjects([]ObjectRef{f2})
	fmt.Printf("[Go] f2: %s\n", f2res)
	return res
}

func task2(data []byte) []byte {
	fmt.Println("[Go] task2")
	time.Sleep(1 * time.Second)
	return []byte(fmt.Sprintf("%v", data))
}

func main() {
	fmt.Printf("[Go] main\n")
}
