package ray

import (
	"bytes"
	"github.com/ray4go/go-ray/ray/ffi"
	"github.com/ray4go/go-ray/ray/utils/log"
	"encoding/binary"
	"fmt"
	"reflect"
	"runtime/debug"
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

type DummyActor struct {
	pyLocalId int64
	typ       *actorType
}

var (
	actorTypes     = make([]*actorType, 0)
	actorsName2Idx = make(map[string]int)

	actorInstances = make([]*actorInstance, 0)
)

// 传入actor类型的指针
// todo: 考虑使用 New(...) (pointer, error) 签名作为构造函数
func RegisterActors(actorFactories map[string]any) {
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
		log.Printf("RegisterActors %s %v, methods %v\n", name, typ.String(), methodName2Idx)
	})

}

func NewActor(typeName string, argsAndOpts ...any) *DummyActor {
	actorIndex, ok := actorsName2Idx[typeName]
	if !ok {
		panic(fmt.Sprintf("Actor type '%v' not found", typeName))
	}
	actor := actorTypes[actorIndex]
	args, opts := splitArgsAndOptions(argsAndOpts)
	log.Debug("[Go] NewActor %s %#v\n", typeName, args)

	buffer := bytes.NewBuffer(nil)
	args, objRefs := splitArgsAndObjectRefs(args)
	argData := encodeArgs(NewCallableType(reflect.TypeOf(actor.newFunc), false), args, len(objRefs))
	appendBytesUnit(buffer, argData)
	optData := encodeOptions(opts, objRefs)
	appendBytesUnit(buffer, optData)

	// request bitmap layout (64 bits, LSB first)
	// | cmdId   | actorIndex |
	// | 10 bits | 54 bits   |
	request := Go2PyCmd_NewActor | int64(actorIndex)<<cmdBitsLen
	res, retCode := ffi.CallServer(request, buffer.Bytes())
	if retCode != 0 {
		panic(fmt.Sprintf("Error: NewActor failed: retCode=%v, message=%s", retCode, res))
	}
	id, err := strconv.ParseInt(string(res), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Error: NewActor invald return: %s, expect a number", res))
	}
	return &DummyActor{
		pyLocalId: id,
		typ:       actor,
	}
}

func handleCreateActor(actorTypeIndex int64, data []byte) (resData []byte, retCode int64) {
	actor := actorTypes[actorTypeIndex]
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("handleCreateActor panic: %v\n%s\n", err, debug.Stack())
			retCode = 1
			resData = []byte(fmt.Sprintf("panic when create actor %s: %v", actor.name, err))
		}
	}()

	args, ok := decodeBytesUnits(data)
	if !ok || len(args) == 0 {
		return []byte("Error: handleCreateActor decode args failed"), 1
	}
	posArgs := make(map[int][]byte)
	for i := 1; i < len(args); i++ { // skip first unit, which is raw args
		pos := int(binary.LittleEndian.Uint64(args[i][:8]))
		posArgs[pos] = args[i][8:]
	}

	res := funcCall(
		nil,
		reflect.ValueOf(actor.newFunc),
		NewCallableType(reflect.TypeOf(actor.newFunc), false),
		args[0],
		posArgs,
	)
	log.Debug("create actor %v -> %v\n", actor.name, res)

	instance := &actorInstance{
		receiver: res[0],
		typ:      actor,
	}
	instanceId := fmt.Sprintf("%d", len(actorInstances))
	actorInstances = append(actorInstances, instance)
	return []byte(instanceId), 0
}

func (actor *DummyActor) RemoteCall(methodName string, argsAndOpts ...any) ObjectRef {
	methodIdx, ok := actor.typ.methodName2Idx[methodName]
	if !ok {
		panic(fmt.Sprintf(
			"Error: RemoteCall failed: no method %s not found in actor %s",
			methodName, actor.typ.name,
		))
	}
	log.Debug("[Go] RemoteCall %s.%s() %#v\n", actor.typ.name, methodName)

	method := actor.typ.methods[methodIdx]
	args, opts := splitArgsAndOptions(argsAndOpts)
	buffer := bytes.NewBuffer(nil)
	args, objRefs := splitArgsAndObjectRefs(args)
	argData := encodeArgs(NewCallableType(method.Type, true), args, len(objRefs))
	appendBytesUnit(buffer, argData)
	optData := encodeOptions(opts, objRefs)
	appendBytesUnit(buffer, optData)

	// request bitmap layout (64 bits, LSB first)
	// | cmdId   | methodIndex | PyActorId |
	// | 10 bits | 22 bits     |  32 bits  |
	request := Go2PyCmd_ActorMethodCall | int64(methodIdx)<<cmdBitsLen | actor.pyLocalId<<32
	res, retCode := ffi.CallServer(request, buffer.Bytes())
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
		originFunc: method.Type,
	}
}

func handleActorMethodCall(request int64, data []byte) (resData []byte, retCode int64) {
	// methodIndex:22bits, actorGoInstanceIndex:32bits
	actorGoInstanceIndex := request >> 32
	methodIndex := request & ((1 << 22) - 1)
	actorIns := actorInstances[actorGoInstanceIndex]
	method := actorIns.typ.methods[methodIndex]
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("actor method call panic: %v\n%s\n", err, debug.Stack())
			retCode = 1
			resData = []byte(fmt.Sprintf("panic when call %s.%s(): %v", actorIns.typ.name, method.Name, err))
		}
	}()

	args, ok := decodeBytesUnits(data)
	if !ok || len(args) == 0 {
		return []byte("Error: handleActorMethodCall decode args failed"), 1
	}
	posArgs := make(map[int][]byte)
	for i := 1; i < len(args); i++ { // skip first unit, which is raw args
		pos := int(binary.LittleEndian.Uint64(args[i][:8]))
		posArgs[pos] = args[i][8:]
	}
	val := reflect.ValueOf(actorIns.receiver)
	res := funcCall(&val, method.Func, NewCallableType(method.Type, true), args[0], posArgs)
	resData = encodeSlice(res)
	return resData, 0
}
