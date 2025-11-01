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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/ray4go/go-ray/ray/internal/log"
	"github.com/ray4go/go-ray/ray/internal/utils"
	"github.com/ugorji/go/codec"
)

type RemoteCallOption struct {
	Name  string
	Value any
}

type RemoteObjectRef struct {
	Id          int64 `json:"id"`
	AutoRelease bool  `json:"auto_release"`
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
	args, objRefs, opts := splitFuncCallRawArgs(argsAndOpts)
	if callable != nil && !(callable.IsValidArgNum(len(args) + len(objRefs))) {
		panic(fmt.Sprintf(
			"encodeArgs: func/method args length not match, given %v, CallableType: %s",
			len(args)+len(objRefs), callable.Type,
		))
	}

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

// FuncCall calls a function or method with the given arguments.
// when receiverVal is not nil, it's a method call; otherwise it's a function call.
func FuncCall(funcVal reflect.Value, receiverVal *reflect.Value, args []any) []any {
	log.Debug("[Go] FuncCall: %v", funcVal)
	argVals := make([]reflect.Value, 0, len(args)+1)
	if receiverVal != nil {
		argVals = append(argVals, *receiverVal)
	}
	for _, arg := range args {
		if arg == nil {
			// funcVal.Call requires the argument type to be non-nil,
			// noted (*T)(nil) is not nil.
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

// the func call args can be either:
//   - normal arg
//   - RemoteObjectRef
//   - RemoteCallOption
//
// the RemoteCallOption must occur in the last of the args.
// this function splits the args into the above 3 types.
func splitFuncCallRawArgs(items []any) ([]any, map[int]*RemoteObjectRef, []*RemoteCallOption) {
	args := make([]any, 0, len(items))
	objs := make(map[int]*RemoteObjectRef)
	opts := make([]*RemoteCallOption, 0, len(items))
	gotOpt := false
	for idx, item := range items {
		if gotOpt {
			if _, ok := item.(*RemoteCallOption); !ok {
				panic("RemoteCallOption must occur in the last of the args")
			}
		}

		switch v := item.(type) {
		case *RemoteObjectRef:
			if v == nil {
				panic("invalid ObjectRef, got nil")
			}
			objs[idx] = v
		case *RemoteCallOption:
			opts = append(opts, v)
			gotOpt = true
		default:
			args = append(args, item)
		}
	}
	return args, objs, opts
}

func encodeOptions(opts []*RemoteCallOption, objRefs map[int]*RemoteObjectRef) []byte {
	kvs := make(map[string]any)
	for _, opt := range opts {
		kvs[opt.Name] = opt.Value
	}
	kvs["go_ray_arg_pos_to_object"] = objRefs
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

// decode func call arguments with given types
// encoded arguments are given in 2 ways:
//   - posArgs: the arguments data of ObjectRefs, key is the argument position in the function/method
//   - args: rest arguments data, concatenated in order
//
// argument types are given from a getter function, the getter function takes the argument position as input,
// and returns the argument type.
func DecodeWithType(args []byte, posArgs map[int][]byte, typeGetter func(argumentPositionIndex int) reflect.Type) []any {
	buf := bytes.NewBuffer(args)
	argsDec := codec.NewDecoder(buf, &msgpackHandle)
	outs := make([]any, 0)
	posArgs = utils.MapShadowCopy(posArgs)
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
