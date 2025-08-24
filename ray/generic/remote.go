package generic

import (
	"fmt"
	"reflect"

	"github.com/ray4go/go-ray/ray"
)

type ObjSetter interface {
	setObjectRef(*ray.ObjectRef)
}

// retmote task function or actor method
type RemoteFunc[T ObjSetter] struct {
	funcName string
	args     []any
	actor    *ray.ActorHandle
}

func NewRemoteFunc[T ObjSetter](funcName string, args []any, actor ...ray.ActorHandle) *RemoteFunc[T] {
	var actorHandle *ray.ActorHandle
	if len(actor) > 0 {
		actorHandle = &actor[0]
	}
	return &RemoteFunc[T]{
		funcName: funcName,
		args:     args,
		actor:    actorHandle,
	}
}

// NewPointedValue 为 T 指向的底层类型创建一个新值，并返回该值的指针
// T 是一个指针类型，例如 *int, *string, *MyStruct
// 例如，NewPointedValue[*MyStruct]() 将创建一个 MyStruct 类型的零值，并返回指向该值的指针
func NewPointedValue[T any]() T {
	var zero T // zero 的类型是 T (例如 *int)
	tType := reflect.TypeOf(zero)
	if tType.Kind() != reflect.Ptr {
		fmt.Printf("泛型参数 T 必须是一个指针类型，但传入的是 %s", tType.Kind())
	}
	// 获取 T 所指向的实际类型 (例如，如果 T 是 *int，那么 elementType 是 int)
	elementType := tType.Elem()
	// 创建该类型的新值
	ptr := reflect.New(elementType).Interface()
	// 类型断言，将 ptr 转换为 T
	return ptr.(T)
}

func (r *RemoteFunc[T]) Remote(options ...*ray.RayOption) T {
	args := ExpandArgs(r.args, options)
	var obj ray.ObjectRef
	if r.actor == nil {
		obj = ray.RemoteCall(r.funcName, args...)
	} else {
		obj = r.actor.RemoteCall(r.funcName, args...)
	}
	t := NewPointedValue[T]()
	t.setObjectRef(&obj)
	return t
}

func ExpandArgs[T any](s1 []any, s2 []T) []any {
	for _, v := range s2 {
		s1 = append(s1, v)
	}
	return s1
}

// Convert 将输入类型转换成底层类型相同的目标类型
func Convert[T any](input any) T {
	srcVal := reflect.ValueOf(input)
	var dst T
	dstType := reflect.TypeOf(dst) // 目标类型
	if !srcVal.Type().ConvertibleTo(dstType) {
		panic(fmt.Sprintf("type %s not convertible to %s", srcVal.Type(), dstType))
	}
	s2Val := srcVal.Convert(dstType)
	return s2Val.Interface().(T)
}

type RemoteActor[T any] struct {
	factoryName string
	args        []any
}

func NewRemoteActor[T any](factoryName string, args []any) *RemoteActor[T] {
	return &RemoteActor[T]{
		factoryName: factoryName,
		args:        args,
	}
}

func (r *RemoteActor[T]) Remote(options ...*ray.RayOption) T {
	args := ExpandArgs(r.args, options)
	handle := ray.NewActor(r.factoryName, args...)
	return Convert[T](*handle)
}
