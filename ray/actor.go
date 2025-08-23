package ray

import (
	"github.com/ray4go/go-ray/ray/internal/consts"
	"github.com/ray4go/go-ray/ray/internal/ffi"
	"github.com/ray4go/go-ray/ray/internal/log"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

type actorType struct {
	name    string
	newFunc any
	methods map[string]reflect.Method
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
	actorTypes     = make(map[string]*actorType)
	actorInstances = make([]*actorInstance, 0)
)

// 传入actor类型的指针
// todo: 考虑使用 New(...) (pointer, error) 签名作为构造函数
func registerActors(actorFactories map[string]any) {
	mapOrderedIterate(actorFactories, func(name string, actorNewFunc any) {
		typ := getFuncReturnType(actorNewFunc)

		methods := make(map[string]reflect.Method)
		for _, method := range getExportedMethods(typ) {
			methods[method.Name] = method
		}
		actor := &actorType{
			name:    name,
			newFunc: actorNewFunc,
			methods: methods,
		}
		actorTypes[actor.name] = actor
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

	actor, ok := actorTypes[typeName]
	if !ok {
		panic(fmt.Sprintf("Actor type '%v' not found", typeName))
	}
	callable := newCallableType(reflect.TypeOf(actor.newFunc), false)
	argsAndOpts = append(argsAndOpts, Option(consts.GorayOptionKey_ActorName, typeName))

	methodNames := make([]string, 0)
	for name := range actor.methods {
		methodNames = append(methodNames, name)
	}
	argsAndOpts = append(argsAndOpts, Option(consts.GorayOptionKey_ActorMethodList, methodNames))
	argsData := encodeRemoteCallArgs(callable, argsAndOpts)

	res, retCode := ffi.CallServer(consts.Go2PyCmd_NewActor, argsData)
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

func handleCreateActor(_ int64, data []byte) (resData []byte, retCode int64) {
	typeName, rawArgs, posArgs := unpackRemoteCallArgs(data)
	actor, ok := actorTypes[typeName]
	if !ok {
		panic(fmt.Sprintf("Actor type '%v' not found", typeName))
	}
	args := decodeWithType(rawArgs, posArgs, newCallableType(reflect.TypeOf(actor.newFunc), false).InType)
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
	var originFunc reflect.Type
	if actor.isGoActor() {
		method, ok := actor.typ.methods[methodName]
		if !ok {
			panic(fmt.Sprintf(
				"Error: RemoteCall failed: no method %s not found in actor %s",
				methodName, actor.typ.name,
			))
		}
		log.Debug("[Go] RemoteCall %s.%s()\n", actor.typ.name, methodName)
		callable = newCallableType(method.Type, true)
		originFunc = method.Type
	} else {
		log.Debug("[Go] RemoteCallPyActor %s() %#v\n", methodName, argsAndOpts)
		originFunc = dummyPyFunc
	}
	argsAndOpts = append(argsAndOpts, Option(consts.GorayOptionKey_TaskName, methodName))
	argsAndOpts = append(argsAndOpts, Option(consts.GorayOptionKey_PyLocalActorId, actor.pyLocalId))
	argsData := encodeRemoteCallArgs(callable, argsAndOpts)

	res, retCode := ffi.CallServer(consts.Go2PyCmd_ActorMethodCall, argsData)
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

func handleGetActorMethods(_ int64, methodName []byte) ([]byte, int64) {
	actorTyp, ok := actorTypes[string(methodName)]
	if !ok {
		return []byte(fmt.Sprintf("Error: golang actor %s not found", methodName)), consts.ErrorCode_Failed
	}
	methodNames := make([]string, 0, len(actorTyp.methods))
	for name := range actorTyp.methods {
		methodNames = append(methodNames, name)
	}
	data, err := json.Marshal(methodNames)
	if err != nil {
		return []byte(fmt.Sprintf("Error: handleGetActorMethods json.Marshal failed: %v", err)), consts.ErrorCode_Failed
	}
	return data, 0
}

func handleActorMethodCall(actorGoInstanceIndex int64, data []byte) (resData []byte, retCode int64) {
	methodName, rawArgs, posArgs := unpackRemoteCallArgs(data)
	actorIns := actorInstances[actorGoInstanceIndex]

	if actorIns == nil {
		return []byte("the actor is died"), consts.ErrorCode_Failed
	}
	method := actorIns.typ.methods[methodName]
	args := decodeWithType(rawArgs, posArgs, newCallableType(method.Type, true).InType)
	receiverVal := reflect.ValueOf(actorIns.receiver)
	res := funcCall(&receiverVal, method.Func, args)
	resData = encodeSlice(res)
	return resData, consts.ErrorCode_Success
}

func handleCloseActor(actorGoInstanceIndex int64, data []byte) (resData []byte, retCode int64) {
	log.Debug("handleCloseActor %d\n", actorGoInstanceIndex)
	actorIns := actorInstances[actorGoInstanceIndex]
	if actorIns == nil {
		return []byte("the actor is already closed"), consts.ErrorCode_Failed
	}
	actorInstances[actorGoInstanceIndex] = nil
	return []byte(""), consts.ErrorCode_Success
}

// Kill an actor forcefully.
//
// See [Ray doc] for more details.
//
// [Ray doc]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.kill.html#ray.kill
func (actor *ActorHandle) Kill(opts ...*RayOption) error {
	opts = append(opts, Option(consts.GorayOptionKey_PyLocalActorId, actor.pyLocalId))
	data, err := jsonEncodeOptions(opts)
	if err != nil {
		return fmt.Errorf("error to json encode ray RayOption: %w", err)
	}

	res, retCode := ffi.CallServer(consts.Go2PyCmd_KillActor, data)

	if retCode != consts.ErrorCode_Success {
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
func GetActor(name string, opts ...*RayOption) (*ActorHandle, error) {
	data, err := jsonEncodeOptions(opts, Option("name", name))
	if err != nil {
		return nil, fmt.Errorf("error to json encode ray RayOption: %w", err)
	}
	resData, retCode := ffi.CallServer(consts.Go2PyCmd_GetActor, data)

	if retCode != consts.ErrorCode_Success {
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
		var ok bool
		actorTyp, ok = actorTypes[res.ActorTypeName]
		if !ok {
			panic(fmt.Sprintf("Actor type '%v' not found", res.ActorTypeName))
		}
	}
	return &ActorHandle{
		pyLocalId: res.PyLocalId,
		typ:       actorTyp,
	}, nil
}
