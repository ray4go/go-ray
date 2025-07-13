package generic

import (
	"fmt"
	"reflect"

	ray "github.com/ray4go/go-ray"
)

type ObjSetter interface {
	setObjectRef(*ray.ObjectRef)
}

type RemoteFunc[T ObjSetter] struct {
	funcName string
	args     []any
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

func (r *RemoteFunc[T]) Remote(options ...ray.TaskOption) T {
	var argsAndOpts []any = r.args
	for _, opt := range options {
		argsAndOpts = append(argsAndOpts, opt)
	}
	obj := ray.RemoteCall(r.funcName, argsAndOpts...)
	t := NewPointedValue[T]()
	t.setObjectRef(&obj)
	return t
}
