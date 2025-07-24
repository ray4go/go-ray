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
func RegisterActors(actors_ []any) {
	for _, actorNewFunc := range actors_ {
		typ := getFuncReturnType(actorNewFunc)
		if _, exists := actorsName2Idx[typ.String()]; exists {
			panic(fmt.Sprintf("Duplcated actor name '%v'", typ.Name()))
		}
		log.Printf("RegisterActors %v\n", typ.String())
		methods := getExportedMethods(typ)
		methodName2Idx := make(map[string]int)
		for i, method := range methods {
			methodName2Idx[method.Name] = i
		}
		actor := &actorType{
			name:           typ.String(),
			newFunc:        actorNewFunc,
			methods:        methods,
			methodName2Idx: methodName2Idx,
		}
		actorTypes = append(actorTypes, actor)
		actorsName2Idx[actor.name] = len(actorTypes) - 1
		log.Printf("RegisterActors %v %v\n", typ.String(), methods)
	}
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

func (actor *DummyActor) RemoteCall(methodName string, argsAndOpts ...any) ObjectRef {
	return ObjectRef{}
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
		receiver: res,
		typ:      actor,
	}
	instanceId := fmt.Sprintf("%d", len(actorInstances))
	actorInstances = append(actorInstances, instance)
	return []byte(instanceId), 0
}
