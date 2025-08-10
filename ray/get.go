package ray

import "fmt"

type decodable interface {
	GetInto(ptrs ...any) error
}

func packArgs(timeout []float64, ptrs ...any) []any {
	timeoutVal := float64(-1)
	if len(timeout) > 0 {
		if len(timeout) != 1 {
			panic(fmt.Sprintf("ObjectRef Get: at most 1 timeout value is allowed, got %v", len(timeout)))
		}
		timeoutVal = timeout[0]
	}
	return append(ptrs, timeoutVal)
}

// Get0 is used to wait remote task / actor method execution finish.
// The optional timeout (in seconds) is only applicable for remote tasks / actors.
func Get0(obj decodable, timeout ...float64) error {
	return obj.GetInto(packArgs(timeout)...)
}

// Get1 is used to get the result of task / actor method with 1 return value.
// The optional timeout (in seconds) is only applicable for remote tasks / actors.
func Get1[T0 any](obj decodable, timeout ...float64) (T0, error) {
	var (
		r0 T0
	)
	err := obj.GetInto(packArgs(timeout, &r0)...)
	return r0, err
}

// Get2 is used to get the result of task / actor method with 2 return value.
// The optional timeout (in seconds) is only applicable for remote tasks / actors.
func Get2[T0 any, T1 any](obj decodable, timeout ...float64) (T0, T1, error) {
	var (
		r0 T0
		r1 T1
	)
	err := obj.GetInto(packArgs(timeout, &r0, &r1)...)
	return r0, r1, err
}

// Get3 is used to get the result of task / actor method with 3 return value.
// The optional timeout (in seconds) is only applicable for remote tasks / actors.
func Get3[T0 any, T1 any, T2 any](obj decodable, timeout ...float64) (T0, T1, T2, error) {
	var (
		r0 T0
		r1 T1
		r2 T2
	)
	err := obj.GetInto(packArgs(timeout, &r0, &r1, &r2)...)
	return r0, r1, r2, err
}

// Get4 is used to get the result of task / actor method with 4 return value.
// The optional timeout (in seconds) is only applicable for remote tasks / actors.
func Get4[T0 any, T1 any, T2 any, T3 any](obj decodable, timeout ...float64) (T0, T1, T2, T3, error) {
	var (
		r0 T0
		r1 T1
		r2 T2
		r3 T3
	)
	err := obj.GetInto(packArgs(timeout, &r0, &r1, &r2, &r3)...)
	return r0, r1, r2, r3, err
}

// Get5 is used to get the result of task / actor method with 5 return value.
// The optional timeout (in seconds) is only applicable for remote tasks / actors.
func Get5[T0 any, T1 any, T2 any, T3 any, T4 any](obj decodable, timeout ...float64) (T0, T1, T2, T3, T4, error) {
	var (
		r0 T0
		r1 T1
		r2 T2
		r3 T3
		r4 T4
	)
	err := obj.GetInto(packArgs(timeout, &r0, &r1, &r2, &r3, &r4)...)
	return r0, r1, r2, r3, r4, err
}
