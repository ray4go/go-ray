package ray

import (
	"github.com/ray4go/go-ray/ray/ffi"
	"github.com/ray4go/go-ray/ray/internal"
	"github.com/ray4go/go-ray/ray/utils/log"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

type actorType struct {
	name           string
	newFunc        any
	methods        []reflect.Method
	methodName2Idx map[string]int
}

type actorInstance struct {
	receiver any
	typ      *actorType
}

// A handle to an actor.
//
// Noted, currently, you can't pass an actor handle into a task.
// Workaround: create a named actor via [NewActor](typeName, [ray.Option]("name", name))
// and use [GetActor](name) to get the actor handle in other tasks.
type ActorHandle struct {
	pyLocalId int64      // actor instance id in py side
	typ       *actorType // nil if the actor is a Python actor
}

var (
	actorTypes     = make([]*actorType, 0)
	actorsName2Idx = make(map[string]int)

	actorInstances = make([]*actorInstance, 0)
)

// 传入actor类型的指针
// todo: 考虑使用 New(...) (pointer, error) 签名作为构造函数
func registerActors(actorFactories map[string]any) {
	mapOrderedIterate(actorFactories, func(name string, actorNewFunc any) {
		typ := getFuncReturnType(actorNewFunc)

		methods := getExportedMethods(typ)
		methodName2Idx := make(map[string]int)
		for i, method := range methods {
			methodName2Idx[method.Name] = i
		}
		actor := &actorType{
			name:           name,
			newFunc:        actorNewFunc,
			methods:        methods,
			methodName2Idx: methodName2Idx,
		}
		actorTypes = append(actorTypes, actor)
		actorsName2Idx[actor.name] = len(actorTypes) - 1
		//log.Printf("RegisterActors %s %v, methods %v\n", name, typ.String(), methodName2Idx)
	})
}

// NewActor creates a remote actor instance of the given type with the provided arguments.
// Ray actor configurations can be passed in the last with [Option](key, value).
//
// For complete options for actor creation, see [Ray Core API doc].
//
// [Ray Core API doc]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.actor.ActorClass.options.html#ray.actor.ActorClass.options
func NewActor(typeName string, argsAndOpts ...any) *ActorHandle {
	log.Debug("[Go] NewActor %s %#v\n", typeName, argsAndOpts)

	actorIndex, ok := actorsName2Idx[typeName]
	if !ok {
		panic(fmt.Sprintf("Actor type '%v' not found", typeName))
	}
	actor := actorTypes[actorIndex]
	callable := newCallableType(reflect.TypeOf(actor.newFunc), false)
	argsAndOpts = append(argsAndOpts, Option("goray_actor_type_name", typeName))
	argsData := encodeRemoteCallArgs(callable, argsAndOpts)

	// request bitmap layout (64 bits, LSB first)
	// | cmdId   | actorIndex |
	// | 10 bits | 54 bits   |
	request := internal.Go2PyCmd_NewActor | int64(actorIndex)<<internal.CmdBitsLen
	res, retCode := ffi.CallServer(request, argsData)
	if retCode != 0 {
		panic(fmt.Sprintf("Error: NewActor failed: retCode=%v, message=%s", retCode, res))
	}
	id, err := strconv.ParseInt(string(res), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Error: NewActor invald return: %s, expect a number", res))
	}
	return &ActorHandle{
		pyLocalId: id,
		typ:       actor,
	}
}

func handleCreateActor(actorTypeIndex int64, data []byte) (resData []byte, retCode int64) {
	actor := actorTypes[actorTypeIndex]
	args := decodeRemoteCallArgs(newCallableType(reflect.TypeOf(actor.newFunc), false), data)
	res := funcCall(nil, reflect.ValueOf(actor.newFunc), args)
	log.Debug("create actor %v -> %v\n", actor.name, res)

	instance := &actorInstance{
		receiver: res[0],
		typ:      actor,
	}
	// instanceId should always be 0 when created from ray.NewActor(),
	// because one actor will occupy one worker process exclusively.
	instanceId := fmt.Sprintf("%d", len(actorInstances))
	//log.Printf("create actor %v, instanceId %v\n", actor.name, instanceId)
	actorInstances = append(actorInstances, instance)
	return []byte(instanceId), 0
}

func (actor *ActorHandle) isGoActor() bool {
	// If actor.typ is nil, it is a Python actor.
	return actor.typ != nil
}

// RemoteCall calls a remote actor method by method name with the given arguments.
// The usage is same as [ray.RemoteCall] except the available options.
// The complete options for actor method call can be found in [Ray Core API doc].
//
// [Ray Core API doc]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.method.html#ray.method
func (actor *ActorHandle) RemoteCall(methodName string, argsAndOpts ...any) ObjectRef {
	var callable *callableType
	var methodIdx int
	var originFunc reflect.Type
	if actor.isGoActor() {
		var ok bool
		methodIdx, ok = actor.typ.methodName2Idx[methodName]
		if !ok {
			panic(fmt.Sprintf(
				"Error: RemoteCall failed: no method %s not found in actor %s",
				methodName, actor.typ.name,
			))
		}
		log.Debug("[Go] RemoteCall %s.%s()\n", actor.typ.name, methodName)
		method := actor.typ.methods[methodIdx]
		callable = newCallableType(method.Type, true)
		originFunc = method.Type
	} else {
		argsAndOpts = append(argsAndOpts, Option("__py_actor_method_name__", methodName))
		log.Debug("[Go] RemoteCallPyActor %s() %#v\n", methodName, argsAndOpts)
		methodIdx = 0 // negative is not allowed, the bit shift operation is not trivial
		originFunc = dummyPyFunc
	}
	argsData := encodeRemoteCallArgs(callable, argsAndOpts)

	// request bitmap layout (64 bits, LSB first)
	// | cmdId   | methodIndex | PyActorInstanceId |
	// | 10 bits | 22 bits     |  32 bits          |
	request := internal.Go2PyCmd_ActorMethodCall | int64(methodIdx)<<internal.CmdBitsLen | actor.pyLocalId<<32
	res, retCode := ffi.CallServer(request, argsData)
	if retCode != 0 {
		// todo: pass error to ObjectRef
		panic(fmt.Sprintf("Error: RemoteCall failed: retCode=%v, message=%s", retCode, res))
	}
	id, err := strconv.ParseInt(string(res), 10, 64) // todo: pass error to ObjectRef
	if err != nil {
		panic(fmt.Sprintf("Error: RemoteCall invald return: %s, expect a number", res))
	}

	return ObjectRef{
		id:         id,
		originFunc: originFunc,
	}
}

func handleActorMethodCall(request int64, data []byte) (resData []byte, retCode int64) {
	// methodIndex:22bits, actorGoInstanceIndex:32bits
	actorGoInstanceIndex := request >> 22
	methodIndex := request & ((1 << 22) - 1)
	actorIns := actorInstances[actorGoInstanceIndex]

	if actorIns == nil {
		return []byte("the actor is died"), internal.ErrorCode_Failed
	}
	method := actorIns.typ.methods[methodIndex]
	args := decodeRemoteCallArgs(newCallableType(method.Type, true), data)
	receiverVal := reflect.ValueOf(actorIns.receiver)
	res := funcCall(&receiverVal, method.Func, args)
	resData = encodeSlice(res)
	return resData, internal.ErrorCode_Success
}

func handleCloseActor(actorGoInstanceIndex int64, data []byte) (resData []byte, retCode int64) {
	log.Debug("handleCloseActor %d\n", actorGoInstanceIndex)
	actorIns := actorInstances[actorGoInstanceIndex]
	if actorIns == nil {
		return []byte("the actor is already closed"), internal.ErrorCode_Failed
	}
	actorInstances[actorGoInstanceIndex] = nil
	return []byte(""), internal.ErrorCode_Success
}

// Kill an actor forcefully.
//
// See [Ray doc] for more details.
//
// [Ray doc]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.kill.html#ray.kill
func (actor *ActorHandle) Kill(opts ...*option) error {
	data, err := jsonEncodeOptions(opts)
	if err != nil {
		return fmt.Errorf("error to json encode ray option: %w", err)
	}

	request := internal.Go2PyCmd_KillActor | int64(actor.pyLocalId)<<internal.CmdBitsLen
	res, retCode := ffi.CallServer(request, data)

	if retCode != internal.ErrorCode_Success {
		return fmt.Errorf("actor.Kill failed, reason: %w, detail: %s", newError(retCode), res)
	}
	return nil
}

// GetActor returns the actor handle by actor name.
// Noted that the actor name is set by passing [ray.Option]("name", name) to [NewActor]().
//
// More supported options can be found in [Ray doc].
//
// [Ray doc]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.get_actor.html#ray.get_actor
func GetActor(name string, opts ...*option) (*ActorHandle, error) {
	data, err := jsonEncodeOptions(opts, Option("name", name))
	if err != nil {
		return nil, fmt.Errorf("error to json encode ray option: %w", err)
	}
	resData, retCode := ffi.CallServer(internal.Go2PyCmd_GetActor, data)

	if retCode != internal.ErrorCode_Success {
		return nil, fmt.Errorf("GetActor failed, reason: %w, detail: %s", newError(retCode), resData)
	}

	var res struct {
		PyLocalId     int64  `json:"py_local_id"`
		ActorTypeName string `json:"actor_type_name"`
		IsGolangActor bool   `json:"is_golang_actor"`
	}

	err = json.Unmarshal(resData, &res)
	if err != nil {
		return nil, fmt.Errorf("json.Unmarshal failed, reason: %w, detail: %s", err, resData)
	}
	var actorTyp *actorType
	if res.IsGolangActor {
		actorTyp = actorTypes[actorsName2Idx[res.ActorTypeName]]
	}
	return &ActorHandle{
		pyLocalId: res.PyLocalId,
		typ:       actorTyp,
	}, nil
}
