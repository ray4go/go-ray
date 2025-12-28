package ray

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/ray4go/go-ray/ray/internal/consts"
	"github.com/ray4go/go-ray/ray/internal/ffi"
	"github.com/ray4go/go-ray/ray/internal/log"
	"github.com/ray4go/go-ray/ray/internal/remote_call"
)

var anyType = reflect.TypeOf((*any)(nil)).Elem()

// RemoteCallPyTask executes a remote Python ray task.
//
// See [GoRay Cross-Language Programming] for more details.
//
// [GoRay Cross-Language Programming]: https://github.com/ray4go/go-ray/blob/master/docs/crosslang.md
func RemoteCallPyTask(name string, argsAndOpts ...any) *ObjectRef {
	log.Debug("[Go] RemoteCallPyTask %s %#v\n", name, argsAndOpts)
	argsAndOpts = append(argsAndOpts, Option("task_name", name))
	argsData := remote_call.EncodeRemoteCallArgs(nil, remoteCallArgs(argsAndOpts))
	res, retCode := ffi.CallServer(consts.Go2PyCmd_ExePythonRemoteTask, argsData) // todo: pass error to ObjectRef
	if retCode != 0 {
		panic(fmt.Sprintf("Error: RemoteCallPyTask failed: retCode=%v, message=%s", retCode, res))
	}
	return &ObjectRef{
		id:          int64(binary.LittleEndian.Uint64(res)),
		types:       []reflect.Type{anyType},
		autoRelease: true,
	}
}

// NewPyActor initializes a new remote Python actor.
//
// See [GoRay Cross-Language Programming] for more details.
//
// [GoRay Cross-Language Programming]: https://github.com/ray4go/go-ray/blob/master/docs/crosslang.md
func NewPyActor(className string, argsAndOpts ...any) *ActorHandle {
	log.Debug("[Go] NewPyActor %s %#v\n", className, argsAndOpts)
	argsAndOpts = append(argsAndOpts, Option(consts.GorayOptionKey_ActorName, className))
	argsData := remote_call.EncodeRemoteCallArgs(nil, remoteCallArgs(argsAndOpts))

	res, retCode := ffi.CallServer(consts.Go2PyCmd_NewPythonActor, argsData)
	if retCode != 0 {
		panic(fmt.Sprintf("Error: NewActor failed: retCode=%v, message=%s", retCode, res))
	}
	return &ActorHandle{
		pyLocalId: int64(binary.LittleEndian.Uint64(res)),
		typ:       nil,
	}
}
