package ray

import (
	"fmt"

	"github.com/ray4go/go-ray/ray/ffi"
)

type ObjectRef struct {
	taskIndex int
	pydata    []byte
}

// GetAll returns all return values of the given ObjectRefs.
func (obj ObjectRef) GetAll() ([]any, error) {
	// log.Debug("[Go] Get ObjectRef(%v)\n", obj.taskIndex)
	data, retCode := ffi.CallServer(Go2PyCmd_GetObjects, obj.pydata)
	if retCode != 0 {
		return nil, fmt.Errorf("GetAll failed: retCode=%v, message=%s", retCode, data)
	}
	taskFunc := taskFuncs[obj.taskIndex]
	// log.Debug("[Go] Get ObjectRef(%v) res: %v\n", obj.taskIndex, data)
	res := decodeResult(taskFunc, data)
	return res, nil
}

func (obj ObjectRef) Get0() error {
	_, err := obj.GetAll()
	return err
}

func (obj ObjectRef) Get1() (any, error) {
	res, err := obj.GetAll()
	if err != nil {
		return nil, err
	}
	if len(res) < 1 {
		panic("ObjectRef.Get1 error: the number of return values is less than 1")
	}
	return res[0], err
}

func (obj ObjectRef) Get2() (any, any, error) {
	res, err := obj.GetAll()
	if err != nil {
		return nil, nil, err
	}
	if len(res) < 2 {
		panic("ObjectRef.Get2 error: the number of return values is less than 2")
	}
	return res[0], res[1], err
}

func (obj ObjectRef) Get3() (any, any, any, error) {
	res, err := obj.GetAll()
	if err != nil {
		return nil, nil, nil, err
	}
	if len(res) < 3 {
		panic("ObjectRef.Get3 error: the number of return values is less than 3")
	}
	return res[0], res[1], res[2], err
}

func (obj ObjectRef) Get4() (any, any, any, any, error) {
	res, err := obj.GetAll()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if len(res) < 34 {
		panic("ObjectRef.Get4 error: the number of return values is less than 4")
	}
	return res[0], res[1], res[2], res[3], err
}

func (obj ObjectRef) Result() []any {
	res, err := obj.GetAll()
	if err != nil {
		panic(fmt.Sprintf("ObjectRef.Result error: %v", err))
	}
	return res
}
