package generic

import (
	"errors"
	"log"
	"reflect"

	ray "github.com/ray4go/go-ray"
)

type Future1[T any] struct {
	obj *ray.ObjectRef
}

func (f *Future1[T]) setObjectRef(obj *ray.ObjectRef) {
	f.obj = obj
}

func (f *Future1[T]) Get() (T, error) {
	// assert len(res) == 1
	res, err := f.obj.GetAll()
	return res[0].(T), err
}

type FutureExt1[T any] struct {
	Future1[T]
}

func (f *FutureExt1[T]) Result() error {
	res, err1 := f.obj.GetAll()
	err2, ok := res[0].(error)
	if !ok {
		log.Panicf("type `%v` is not error type, please use Get() instead", reflect.TypeOf(res[0]))
	}
	return errors.Join(err1, err2)
}

type Future2[T0 any, T1 any] struct {
	obj *ray.ObjectRef
}

func (f *Future2[T0, T1]) setObjectRef(obj *ray.ObjectRef) {
	f.obj = obj
}

func (f *Future2[T0, T1]) Get() (T0, T1, error) {
	res, err := f.obj.GetAll()
	return res[0].(T0), res[1].(T1), err
}

type FutureExt2[T0 any, T1 any] struct {
	Future2[T0, T1]
}

// future表示一次函数在ray上异步执行的返回值，Get()可以等待函数运行完成并获取其返回值，同时返回值中还加入了一个新的error类型的返回值，用于表示框架执行过程中是否发生了错误。
// 如果用户的函数返回值也包含error类型，为了不让用户在代码中处理多个error，在future引入了一个新的方法Result()，它会将用户函数返回值中的error和框架执行过程中的error合并成一个error返回。

func (f *FutureExt2[T0, T1]) Result() (T0, error) {
	res, err1 := f.obj.GetAll()
	err2, ok := res[1].(error)
	if !ok {
		log.Panicf("type `%v` is not error type, please use Get() instead", reflect.TypeOf(res[1]))
	}
	return res[0].(T0), errors.Join(err1, err2)
}
