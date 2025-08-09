/*
Remote call flow:
1. encode func/method args & ray options
2. decode func/method args & ray options
3. call func/method
4. encode result
5. decode result
*/

package ray

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/ugorji/go/codec"
	"reflect"

	"github.com/ray4go/go-ray/ray/utils/log"
)

/*
remote call args 包含
  - 函数调用的普通参数
  - 函数调用的 ObjectRef 参数
  - ray options

encode result: 2 bytes units
  - first unit is raw args data;
  - second unit is json encoded options and ObjectRef info.
*/
func encodeRemoteCallArgs(callable *callableType, argsAndOpts []any) []byte {
	argsAndObjs, opts := splitArgsAndOptions(argsAndOpts)
	args, objRefs := splitArgsAndObjectRefs(argsAndObjs)
	if callable != nil && !(callable.IsValidArgNum(len(args) + len(objRefs))) {
		panic(fmt.Sprintf(
			"encodeArgs: func/method args length not match, given %v, callableType: %s",
			len(args)+len(objRefs), callable.Type,
		))
	}

	argData := encodeSlice(args)
	buffer := bytes.NewBuffer(nil)
	appendBytesUnit(buffer, argData)
	optData := encodeOptions(opts, objRefs)
	appendBytesUnit(buffer, optData)
	return buffer.Bytes()
}

/*
data format: multiple bytes units
bytes unit format: | length:8byte:int64 | data:${length}byte:[]byte |

- first unit is raw args data;
- other units are objectRefs resolved data;
  - resolved data format: | arg_pos:8byte:int64 | data:[]byte |
*/
func decodeRemoteCallArgs(callable *callableType, data []byte) []any {
	args, ok := decodeBytesUnits(data)
	if !ok || len(args) == 0 {
		panic("Error: decode args of remote call failed")
	}
	rawArgs := args[0]
	objUnits := args[1:]

	posArgs := make(map[int][]byte)
	for _, unit := range objUnits {
		pos := int(binary.LittleEndian.Uint64(unit[:8]))
		posArgs[pos] = unit[8:]
	}

	return decodeWithType(rawArgs, posArgs, callable.InType)
}

func funcCall(receiverVal *reflect.Value, funcVal reflect.Value, args []any) []any {
	log.Debug("[Go] funcCall: %v", funcVal)
	argVals := make([]reflect.Value, 0, len(args)+1)
	if receiverVal != nil {
		argVals = append(argVals, *receiverVal)
	}
	for _, arg := range args {
		if arg == nil {
			// funcVal.Call requires the argument type to be non-nil,
			// so we use a zero value of the type.
			// todo: use a zero value of the type.
			//arg = reflect.New(reflect.TypeOf(anyNil)).Elem()
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

func encodeFuncResult(results []any) []byte {
	return encodeSlice(results)
}

func decodeFuncResult(funcType reflect.Type, rawResult []byte) []any {
	return decodeWithType(rawResult, nil, funcType.Out)
}

func splitArgsAndObjectRefs(items []any) ([]any, map[int]ObjectRef) {
	args := make([]any, 0, len(items))
	objs := make(map[int]ObjectRef)
	for idx, item := range items {
		switch v := item.(type) {
		case ObjectRef:
			objs[idx] = v
		case *ObjectRef:
			if v == nil {
				panic("invalid ObjectRef, got nil")
			}
			objs[idx] = *v
		default:
			args = append(args, item)
		}
		if obj, ok := objs[idx]; ok {
			if obj.numReturn() != 1 {
				panic(fmt.Sprintf("Error: invalid ObjectRef in arguments[%d], only accept ObjectRef with one return value", idx))
			}
		}
	}
	return args, objs
}

func splitArgsAndOptions(items []any) ([]any, []*option) {
	args := make([]any, 0, len(items))
	opts := make([]*option, 0, len(items))
	for _, item := range items {
		if opt, ok := item.(*option); ok {
			opts = append(opts, opt)
		} else {
			args = append(args, item)
		}
	}
	return args, opts
}

func encodeOptions(opts []*option, objRefs map[int]ObjectRef) []byte {
	kvs := make(map[string]any)
	for _, opt := range opts {
		kvs[opt.Name()] = opt.Value()
	}
	objIdx2Ids := make(map[int]int64)
	for idx, obj := range objRefs {
		objIdx2Ids[idx] = obj.id
	}
	kvs["go_ray_object_pos_to_local_id"] = objIdx2Ids
	data, err := json.Marshal(kvs)
	if err != nil {
		panic(fmt.Sprintf("Error encoding options to JSON: %v", err))
	}
	return data
}

var msgpackHandle = codec.MsgpackHandle{WriteExt: true}

func encodeValue(v any) ([]byte, error) {
	var buffer bytes.Buffer
	enc := codec.NewEncoder(&buffer, &msgpackHandle)
	err := enc.Encode(v)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func encodeSlice(items []any) []byte {
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

func decodeWithType(args []byte, posArgs map[int][]byte, typeGetter func(int) reflect.Type) []any {
	buf := bytes.NewBuffer(args)
	argsDec := codec.NewDecoder(buf, &msgpackHandle)
	outs := make([]any, 0)
	posArgs = copyMap(posArgs)
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

func decodeInto(data []byte, ptrs []any) error {
	vals := make([]reflect.Value, len(ptrs))
	for i, ptr := range ptrs {
		val := reflect.ValueOf(ptr)
		if val.Kind() != reflect.Ptr {
			return fmt.Errorf("input is not a pointer, got %s", val.Kind())
		}
		vals[i] = val
	}
	res := decodeWithType(data, nil, func(i int) reflect.Type {
		return reflect.TypeOf(ptrs[i]).Elem()
	})
	for i, val := range vals {
		val.Elem().Set(reflect.ValueOf(res[i]))
	}
	return nil
}
