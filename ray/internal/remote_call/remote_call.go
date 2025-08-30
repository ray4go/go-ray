/*
Remote call flow:
1. encode func/method args & ray options
2. decode func/method args & ray options
3. call func/method
4. encode result
5. decode result
*/

package remote_call

import (
	"bytes"
	"github.com/ray4go/go-ray/ray/internal/log"
	"github.com/ray4go/go-ray/ray/internal/utils"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/ugorji/go/codec"
	"reflect"
)

type RemoteCallOption struct {
	Name  string
	Value any
}

type RemoteObjectRef struct {
	Id        int64
	NumReturn int
}

/*
remote call args 包含
  - 函数调用的普通参数
  - 函数调用的 ObjectRef 参数
  - ray options

encode result: 2 bytes units
  - first unit is raw args data;
  - second unit is json encoded options and ObjectRef info.
*/
func EncodeRemoteCallArgs(callable *utils.CallableType, argsAndOpts []any) []byte {
	argsAndObjs, opts := splitArgsAndOptions(argsAndOpts)
	if callable != nil && !(callable.IsValidArgNum(len(argsAndObjs))) {
		panic(fmt.Sprintf(
			"encodeArgs: func/method args length not match, given %v, CallableType: %s",
			len(argsAndObjs), callable.Type,
		))
	}

	args, objRefs := splitArgsAndObjectRefs(argsAndObjs)
	argData := EncodeSlice(args)
	buffer := bytes.NewBuffer(nil)
	utils.AppendBytesUnit(buffer, argData)
	optData := encodeOptions(opts, objRefs)
	utils.AppendBytesUnit(buffer, optData)
	return buffer.Bytes()
}

/*
data format: multiple bytes units
bytes unit format: | length:8byte:int64 | data:${length}byte:[]byte |

- first unit is function/actor_type/method name
- second unit is raw args data;
- other units are objectRefs resolved data;
  - resolved data format: | arg_pos:8byte:int64 | data:[]byte |
*/
func UnpackRemoteCallArgs(data []byte) (string, []byte, map[int][]byte) {
	args, ok := utils.DecodeBytesUnits(data)
	if !ok || len(args) == 0 {
		panic("Error: decode args of remote call failed")
	}

	funcName := string(args[0])
	rawArgs := args[1]
	objUnits := args[2:]

	posArgs := make(map[int][]byte)
	for _, unit := range objUnits {
		pos := int(binary.LittleEndian.Uint64(unit[:8]))
		posArgs[pos] = unit[8:]
	}

	return funcName, rawArgs, posArgs
}

func FuncCall(receiverVal *reflect.Value, funcVal reflect.Value, args []any) []any {
	log.Debug("[Go] FuncCall: %v", funcVal)
	argVals := make([]reflect.Value, 0, len(args)+1)
	if receiverVal != nil {
		argVals = append(argVals, *receiverVal)
	}
	for _, arg := range args {
		if arg == nil {
			// funcVal.Call requires the argument type to be non-nil,
			panic("passing nil to interface{} type parameter is not allowed")
		}
		argVals = append(argVals, reflect.ValueOf(arg))
	}
	returnValues := funcVal.Call(argVals)

	results := make([]any, len(returnValues))
	for i, res := range returnValues {
		results[i] = res.Interface()
	}
	return results
}

func EncodeFuncResult(results []any) []byte {
	return EncodeSlice(results)
}

func DecodeFuncResult(funcType reflect.Type, rawResult []byte) []any {
	return DecodeWithType(rawResult, nil, funcType.Out)
}

func splitArgsAndObjectRefs(items []any) ([]any, map[int]RemoteObjectRef) {
	args := make([]any, 0, len(items))
	objs := make(map[int]RemoteObjectRef)
	for idx, item := range items {
		switch v := item.(type) {
		case RemoteObjectRef:
			objs[idx] = v
		case *RemoteObjectRef:
			if v == nil {
				panic("invalid ObjectRef, got nil")
			}
			objs[idx] = *v
		default:
			args = append(args, item)
		}
		if obj, ok := objs[idx]; ok {
			if obj.NumReturn != 1 {
				panic(fmt.Sprintf(
					"Error: invalid ObjectRef in arguments[%d], only accept ObjectRef with one return value."+
						"the ObjectRef you provided has %d return value", idx, obj.NumReturn))
			}
		}
	}
	return args, objs
}

func splitArgsAndOptions(items []any) ([]any, []*RemoteCallOption) {
	args := make([]any, 0, len(items))
	opts := make([]*RemoteCallOption, 0, len(items))
	for _, item := range items {
		if opt, ok := item.(*RemoteCallOption); ok {
			opts = append(opts, opt)
		} else {
			args = append(args, item)
		}
	}
	return args, opts
}

func encodeOptions(opts []*RemoteCallOption, objRefs map[int]RemoteObjectRef) []byte {
	kvs := make(map[string]any)
	for _, opt := range opts {
		kvs[opt.Name] = opt.Value
	}
	objIdx2Ids := make(map[int]int64)
	for idx, obj := range objRefs {
		objIdx2Ids[idx] = obj.Id
	}
	kvs["go_ray_object_pos_to_local_id"] = objIdx2Ids
	data, err := json.Marshal(kvs)
	if err != nil {
		panic(fmt.Sprintf("Error encoding options to JSON: %v", err))
	}
	return data
}

var msgpackHandle = codec.MsgpackHandle{WriteExt: true}

func EncodeValue(v any) ([]byte, error) {
	var buffer bytes.Buffer
	enc := codec.NewEncoder(&buffer, &msgpackHandle)
	err := enc.Encode(v)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func EncodeSlice(items []any) []byte {
	var buffer bytes.Buffer
	enc := codec.NewEncoder(&buffer, &msgpackHandle)
	for _, item := range items {
		err := enc.Encode(item)
		if err != nil {
			log.Panicf("Serialize type %v error: %v", reflect.TypeOf(item), err)
		}
	}
	return buffer.Bytes()
}

func DecodeWithType(args []byte, posArgs map[int][]byte, typeGetter func(int) reflect.Type) []any {
	buf := bytes.NewBuffer(args)
	argsDec := codec.NewDecoder(buf, &msgpackHandle)
	outs := make([]any, 0)
	posArgs = utils.CopyMap(posArgs)
	for idx := 0; buf.Len() > 0 || len(posArgs) > 0; idx++ {
		argData, ok := posArgs[idx]
		var dec *codec.Decoder
		if ok {
			dec = codec.NewDecoderBytes(argData, &msgpackHandle)
			delete(posArgs, idx) // remove the processed pos arg
		} else {
			dec = argsDec
		}
		typ := typeGetter(idx)
		item := reflect.New(typ)
		err := dec.Decode(item.Interface())
		if err != nil {
			log.Panicf("decode type `%v` error: %v", typ, err)
		}
		outs = append(outs, item.Elem().Interface())
	}
	return outs
}

func DecodeInto(data []byte, ptrs []any) error {
	vals := make([]reflect.Value, len(ptrs))
	for i, ptr := range ptrs {
		val := reflect.ValueOf(ptr)
		if val.Kind() != reflect.Ptr {
			return fmt.Errorf("input is not a pointer, got %s", val.Kind())
		}
		vals[i] = val
	}
	res := DecodeWithType(data, nil, func(i int) reflect.Type {
		return reflect.TypeOf(ptrs[i]).Elem()
	})
	for i, val := range vals {
		val.Elem().Set(reflect.ValueOf(res[i]))
	}
	return nil
}
