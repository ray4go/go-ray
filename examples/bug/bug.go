package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"
	"reflect"
)

type ObjectRef struct {
	taskIndex int32
	pydata    []byte
}

func (obj ObjectRef) MarshalBinary() ([]byte, error) {
	data := make([]byte, 4+len(obj.pydata))
	binary.LittleEndian.PutUint32(data, uint32(obj.taskIndex))
	copy(data[4:], obj.pydata)
	return data, nil
}

func (obj *ObjectRef) UnmarshalBinary(data []byte) error {
	obj.taskIndex = int32(binary.LittleEndian.Uint32(data))
	obj.pydata = data[4:]
	return nil
}

func encodeSlice(items []any) []byte {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	for _, item := range items {
		err := enc.Encode(item)
		if err != nil {
			log.Panicf("gob encode type %v error: %v", reflect.TypeOf(item), err)
		}
	}
	return buffer.Bytes()
}

func main() {
	obj := ObjectRef{
		taskIndex: 1,
		pydata:    []byte{1, 2, 3},
	}
	args := []any{obj}
	fmt.Printf("args -> %#v \n", args)

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	for _, item := range args {
		err := enc.Encode(item)
		if err != nil {
			log.Panicf("gob encode type %v error: %v", reflect.TypeOf(item), err)
		}
	}
	fmt.Println("data1", buffer.Bytes())

	fmt.Println("data2", encodeSlice(args))

}
