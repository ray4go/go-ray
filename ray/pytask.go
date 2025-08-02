package ray

import (
	"github.com/ray4go/go-ray/ray/ffi"
	"github.com/ray4go/go-ray/ray/internal"
	"github.com/ray4go/go-ray/ray/utils/log"
	"fmt"
	"reflect"
	"strconv"
)

var dummyPyFunc = reflect.TypeOf(func() any { return nil })

func RemoteCallPyTask(name string, argsAndOpts ...any) ObjectRef {
	log.Debug("[Go] RemoteCallPyTask %s %#v\n", name, argsAndOpts)
	argsAndOpts = append(argsAndOpts, Option("task_name", name))
	argsData := encodeRemoteCallArgs(nil, argsAndOpts)
	request := internal.Go2PyCmd_ExePythonRemoteTask
	res, retCode := ffi.CallServer(int64(request), argsData) // todo: pass error to ObjectRef
	if retCode != 0 {
		panic(fmt.Sprintf("Error: RemoteCallPyTask failed: retCode=%v, message=%s", retCode, res))
	}
	id, err := strconv.ParseInt(string(res), 10, 64) // todo: pass error to ObjectRef
	if err != nil {
		panic(fmt.Sprintf("Error: RemoteCall invald return: %s, expect a number", res))
	}

	return ObjectRef{
		id:         id,
		originFunc: dummyPyFunc,
	}
}

func LocalCallPyTask(name string, args ...any) ([]any, error) {
	log.Debug("[Go] LocalCallPyTask %s %#v\n", name, args)
	// todo: check no objref and option in args
	argsAndOpts := append(args, Option("task_name", name))
	argsData := encodeRemoteCallArgs(nil, argsAndOpts)
	request := internal.Go2PyCmd_ExePythonLocalTask
	resData, retCode := ffi.CallServer(int64(request), argsData) // todo: pass error to ObjectRef
	if retCode != 0 {
		return nil, fmt.Errorf("Error: LocalCallPyTask failed: retCode=%v, message=%s", retCode, resData)
	}
	res := decodeFuncResult(dummyPyFunc, resData)
	return res, nil
}
