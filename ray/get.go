package ray

// Implemented by [ObjectRef] and [LocalPyCallResult]. Used in ray.GetN series functions.
type Decodable interface {
	GetInto(ptrs ...any) error
}

func appendOptions(options []GetObjectOption, ptrs ...any) []any {
	for _, opt := range options {
		ptrs = append(ptrs, opt)
	}
	return ptrs
}

// Get0 is used to wait remote task / actor method execution finish.
// [WithTimeout]() can be used as options to set timeout.
func Get0(obj Decodable, options ...GetObjectOption) error {
	return obj.GetInto(appendOptions(options)...)
}

// Get1 is used to get the result of task / actor method with 1 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get1[T0 any](obj Decodable, options ...GetObjectOption) (T0, error) {
	var (
		r0 T0
	)
	err := obj.GetInto(appendOptions(options, &r0)...)
	return r0, err
}

// Get2 is used to get the result of task / actor method with 2 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get2[T0 any, T1 any](obj Decodable, options ...GetObjectOption) (T0, T1, error) {
	var (
		r0 T0
		r1 T1
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1)...)
	return r0, r1, err
}

// Get3 is used to get the result of task / actor method with 3 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get3[T0 any, T1 any, T2 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, error) {
	var (
		r0 T0
		r1 T1
		r2 T2
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2)...)
	return r0, r1, r2, err
}

// Get4 is used to get the result of task / actor method with 4 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get4[T0 any, T1 any, T2 any, T3 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, T3, error) {
	var (
		r0 T0
		r1 T1
		r2 T2
		r3 T3
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2, &r3)...)
	return r0, r1, r2, r3, err
}

// Get5 is used to get the result of task / actor method with 5 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get5[T0 any, T1 any, T2 any, T3 any, T4 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, T3, T4, error) {
	var (
		r0 T0
		r1 T1
		r2 T2
		r3 T3
		r4 T4
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2, &r3, &r4)...)
	return r0, r1, r2, r3, r4, err
}
