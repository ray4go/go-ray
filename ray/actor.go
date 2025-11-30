package ray

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/ray4go/go-ray/ray/internal/consts"
	"github.com/ray4go/go-ray/ray/internal/ffi"
	"github.com/ray4go/go-ray/ray/internal/log"
	"github.com/ray4go/go-ray/ray/internal/remote_call"
	"github.com/ray4go/go-ray/ray/internal/utils"
)

type actorType struct {
	name                string
	constructorFunc     any
	constructorReceiver reflect.Value             // the actor constructor func is actually a method of this receiver
	methods             map[string]reflect.Method // actor methods
}

type actorInstance struct {
	receiver reflect.Value
	typ      *actorType
}

// ActorHandle represents a handle to an actor.
//
// Note: currently, you can't pass an [ActorHandle] as a parameter to a task.
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
// todo: consider to use New(...) (pointer, error) as the constructor func signature
func registerActors(actorFactories any) {
	actorFactoriesVal := reflect.ValueOf(actorFactories)
	actorFactoriesList := utils.GetExportedMethods(reflect.TypeOf(actorFactories), true)

	for _, method := range actorFactoriesList {
		if method.Type.NumOut() != 1 {
			panic(fmt.Sprintf("Error: constructor of actor %s must return only one value, get %d", method.Name, method.Type.NumOut()))
		}
		actorTyp := method.Type.Out(0)
		methods := make(map[string]reflect.Method)
		for _, actorMethod := range utils.GetExportedMethods(actorTyp, false) {
			methods[actorMethod.Name] = actorMethod
		}

		actor := &actorType{
			name:                method.Name,
			constructorReceiver: actorFactoriesVal,
			constructorFunc:     method.Func.Interface(),
			methods:             methods,
		}
		actorTypes[actor.name] = actor
	}
}

// NewActor initializes a new remote actor.
//   - typeName should match the method name of the actor constructor registered in [Init].
//   - argsAndOpts include actor constructor arguments and Ray actor instantiation options.
//
// Provide Ray actor options via [Option](key, value). For complete options for actor creation, see [Ray Core API doc].
//
// [Ray Core API doc]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.actor.ActorClass.options.html#ray.actor.ActorClass.options
func NewActor(typeName string, argsAndOpts ...any) *ActorHandle {
	log.Debug("[Go] NewActor %s %#v\n", typeName, argsAndOpts)

	actor, ok := actorTypes[typeName]
	if !ok {
		panic(fmt.Sprintf("Actor type '%v' not found", typeName))
	}
	callable := utils.NewCallableType(reflect.TypeOf(actor.constructorFunc), true)
	argsAndOpts = append(argsAndOpts, Option(consts.GorayOptionKey_ActorName, typeName))

	methodNames := make([]string, 0)
	for name := range actor.methods {
		methodNames = append(methodNames, name)
	}
	argsAndOpts = append(argsAndOpts, Option(consts.GorayOptionKey_ActorMethodList, methodNames))
	argsData := remote_call.EncodeRemoteCallArgs(callable, remoteCallArgs(argsAndOpts))

	res, retCode := ffi.CallServer(consts.Go2PyCmd_NewActor, argsData)
	if retCode != 0 {
		panic(fmt.Sprintf("Error: NewActor failed: retCode=%v, message=%s", retCode, res))
	}
	return &ActorHandle{
		pyLocalId: int64(binary.LittleEndian.Uint64(res)),
		typ:       actor,
	}
}

func handleCreateActor(_ int64, data []byte) (resData []byte, retCode int64) {
	typeName, rawArgs, posArgs := remote_call.UnpackRemoteCallArgs(data)
	actor, ok := actorTypes[typeName]
	if !ok {
		panic(fmt.Sprintf("Actor type '%v' not found", typeName))
	}
	args := remote_call.DecodeWithType(rawArgs, posArgs, utils.NewCallableType(reflect.TypeOf(actor.constructorFunc), true).InType)
	res := remote_call.FuncCall(reflect.ValueOf(actor.constructorFunc), &actor.constructorReceiver, args)
	if utils.IsNilTypePointer(res[0]) {
		panic(fmt.Sprintf("Error: create %s actor failed, constructor return nil", typeName))
	}
	log.Debug("create actor %v -> %v\n", actor.name, res)

	instance := &actorInstance{
		receiver: reflect.ValueOf(res[0]),
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
// Usage is the same as [ray.RemoteCall].
// The complete Ray actor method call options can be found in [Ray Core API doc].
//
// [Ray Core API doc]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.method.html#ray.method
func (actor *ActorHandle) RemoteCall(methodName string, argsAndOpts ...any) *ObjectRef {
	var callable *utils.CallableType
	var returnTypes []reflect.Type
	if actor.isGoActor() {
		method, ok := actor.typ.methods[methodName]
		if !ok {
			panic(fmt.Sprintf(
				"Error: RemoteCall failed: no method %s not found in actor %s",
				methodName, actor.typ.name,
			))
		}
		log.Debug("[Go] RemoteCall %s.%s()\n", actor.typ.name, methodName)
		callable = utils.NewCallableType(method.Type, true)
		returnTypes = make([]reflect.Type, 0, method.Type.NumOut())
		for i := 0; i < method.Type.NumOut(); i++ {
			returnTypes = append(returnTypes, method.Type.Out(i))
		}
	} else {
		log.Debug("[Go] RemoteCallPyActor %s() %#v\n", methodName, argsAndOpts)
		returnTypes = []reflect.Type{anyType}
	}
	argsAndOpts = append(argsAndOpts, Option(consts.GorayOptionKey_TaskName, methodName))
	argsAndOpts = append(argsAndOpts, Option(consts.GorayOptionKey_PyLocalActorId, actor.pyLocalId))
	argsData := remote_call.EncodeRemoteCallArgs(callable, remoteCallArgs(argsAndOpts))

	res, retCode := ffi.CallServer(consts.Go2PyCmd_ActorMethodCall, argsData)
	if retCode != 0 {
		// todo: pass error to ObjectRef
		panic(fmt.Sprintf("Error: RemoteCall failed: retCode=%v, message=%s", retCode, res))
	}
	return &ObjectRef{
		id:          int64(binary.LittleEndian.Uint64(res)),
		types:       returnTypes,
		autoRelease: true,
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
	methodName, rawArgs, posArgs := remote_call.UnpackRemoteCallArgs(data)
	actorIns := actorInstances[actorGoInstanceIndex]

	if actorIns == nil {
		return []byte("the actor is died"), consts.ErrorCode_Failed
	}
	method := actorIns.typ.methods[methodName]
	args := remote_call.DecodeWithType(rawArgs, posArgs, utils.NewCallableType(method.Type, true).InType)
	res := remote_call.FuncCall(method.Func, &actorIns.receiver, args)
	resData = remote_call.EncodeSlice(res)
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

func getActor(name string, opts ...*RayOption) (*ActorHandle, string, error) {
	data, err := jsonEncodeOptions(opts, Option("name", name))
	if err != nil {
		return nil, "", fmt.Errorf("error to json encode ray RayOption: %w", err)
	}
	resData, retCode := ffi.CallServer(consts.Go2PyCmd_GetActor, data)

	if retCode != consts.ErrorCode_Success {
		return nil, "", fmt.Errorf("GetActor failed, reason: %w, detail: %s", newError(retCode), resData)
	}

	var res struct {
		PyLocalId     int64  `json:"py_local_id"`
		ActorTypeName string `json:"actor_type_name"`
		IsGolangActor bool   `json:"is_golang_actor"`
	}

	err = json.Unmarshal(resData, &res)
	if err != nil {
		return nil, "", fmt.Errorf("json.Unmarshal failed, reason: %w, detail: %s", err, resData)
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
	}, res.ActorTypeName, nil
}

// GetActor get a handle to a named actor.
//
// Noted that the actor name is set by passing [ray.Option]("name", name) to [NewActor]().
// More supported options can be found in [Ray doc].
//
// [Ray doc]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.get_actor.html#ray.get_actor
func GetActor(name string, opts ...*RayOption) (*ActorHandle, error) {
	handle, _, err := getActor(name, opts...)
	return handle, err
}

// GetTypedActor gets the type-safe actor handle used in [goraygen] wrappers.
//
// [goraygen]: https://github.com/ray4go/goraygen
func GetTypedActor[T any](name string, opts ...*RayOption) (*T, error) {
	handle, actorTypeName, err := getActor(name, opts...)
	if err != nil {
		return nil, err
	}

	var val T
	if expectName := strings.TrimPrefix(reflect.TypeOf(val).Name(), "Actor"); expectName != actorTypeName {
		return nil, fmt.Errorf("actor type mismatch: got %s, want %s", actorTypeName, expectName)
	}
	return utils.NewViaEmbedding[T](handle), nil
}
