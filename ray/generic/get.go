package generic

import (
	. "github.com/ray4go/go-ray/ray"
)

func appendOptions(options []GetObjectOption, ptrs ...any) []any {
	for _, opt := range options {
		ptrs = append(ptrs, opt)
	}
	return ptrs
}

// Get6 is used to get the result of task / actor method with 6 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get6[T0 any, T1 any, T2 any, T3 any, T4 any, T5 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, T3, T4, T5, error) {
	var (
		r0 T0
		r1 T1
		r2 T2
		r3 T3
		r4 T4
		r5 T5
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2, &r3, &r4, &r5)...)
	return r0, r1, r2, r3, r4, r5, err
}

// Get7 is used to get the result of task / actor method with 7 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get7[T0 any, T1 any, T2 any, T3 any, T4 any, T5 any, T6 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, T3, T4, T5, T6, error) {
	var (
		r0 T0
		r1 T1
		r2 T2
		r3 T3
		r4 T4
		r5 T5
		r6 T6
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2, &r3, &r4, &r5, &r6)...)
	return r0, r1, r2, r3, r4, r5, r6, err
}

// Get8 is used to get the result of task / actor method with 8 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get8[T0 any, T1 any, T2 any, T3 any, T4 any, T5 any, T6 any, T7 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, T3, T4, T5, T6, T7, error) {
	var (
		r0 T0
		r1 T1
		r2 T2
		r3 T3
		r4 T4
		r5 T5
		r6 T6
		r7 T7
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2, &r3, &r4, &r5, &r6, &r7)...)
	return r0, r1, r2, r3, r4, r5, r6, r7, err
}

// Get9 is used to get the result of task / actor method with 9 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get9[T0 any, T1 any, T2 any, T3 any, T4 any, T5 any, T6 any, T7 any, T8 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, T3, T4, T5, T6, T7, T8, error) {
	var (
		r0 T0
		r1 T1
		r2 T2
		r3 T3
		r4 T4
		r5 T5
		r6 T6
		r7 T7
		r8 T8
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2, &r3, &r4, &r5, &r6, &r7, &r8)...)
	return r0, r1, r2, r3, r4, r5, r6, r7, r8, err
}

// Get10 is used to get the result of task / actor method with 10 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get10[T0 any, T1 any, T2 any, T3 any, T4 any, T5 any, T6 any, T7 any, T8 any, T9 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, error) {
	var (
		r0 T0
		r1 T1
		r2 T2
		r3 T3
		r4 T4
		r5 T5
		r6 T6
		r7 T7
		r8 T8
		r9 T9
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2, &r3, &r4, &r5, &r6, &r7, &r8, &r9)...)
	return r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, err
}

// Get11 is used to get the result of task / actor method with 11 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get11[T0 any, T1 any, T2 any, T3 any, T4 any, T5 any, T6 any, T7 any, T8 any, T9 any, T10 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, error) {
	var (
		r0  T0
		r1  T1
		r2  T2
		r3  T3
		r4  T4
		r5  T5
		r6  T6
		r7  T7
		r8  T8
		r9  T9
		r10 T10
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2, &r3, &r4, &r5, &r6, &r7, &r8, &r9, &r10)...)
	return r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, err
}

// Get12 is used to get the result of task / actor method with 12 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get12[T0 any, T1 any, T2 any, T3 any, T4 any, T5 any, T6 any, T7 any, T8 any, T9 any, T10 any, T11 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, error) {
	var (
		r0  T0
		r1  T1
		r2  T2
		r3  T3
		r4  T4
		r5  T5
		r6  T6
		r7  T7
		r8  T8
		r9  T9
		r10 T10
		r11 T11
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2, &r3, &r4, &r5, &r6, &r7, &r8, &r9, &r10, &r11)...)
	return r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, err
}

// Get13 is used to get the result of task / actor method with 13 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get13[T0 any, T1 any, T2 any, T3 any, T4 any, T5 any, T6 any, T7 any, T8 any, T9 any, T10 any, T11 any, T12 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, error) {
	var (
		r0  T0
		r1  T1
		r2  T2
		r3  T3
		r4  T4
		r5  T5
		r6  T6
		r7  T7
		r8  T8
		r9  T9
		r10 T10
		r11 T11
		r12 T12
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2, &r3, &r4, &r5, &r6, &r7, &r8, &r9, &r10, &r11, &r12)...)
	return r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, err
}

// Get14 is used to get the result of task / actor method with 14 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get14[T0 any, T1 any, T2 any, T3 any, T4 any, T5 any, T6 any, T7 any, T8 any, T9 any, T10 any, T11 any, T12 any, T13 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, error) {
	var (
		r0  T0
		r1  T1
		r2  T2
		r3  T3
		r4  T4
		r5  T5
		r6  T6
		r7  T7
		r8  T8
		r9  T9
		r10 T10
		r11 T11
		r12 T12
		r13 T13
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2, &r3, &r4, &r5, &r6, &r7, &r8, &r9, &r10, &r11, &r12, &r13)...)
	return r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, err
}

// Get15 is used to get the result of task / actor method with 15 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get15[T0 any, T1 any, T2 any, T3 any, T4 any, T5 any, T6 any, T7 any, T8 any, T9 any, T10 any, T11 any, T12 any, T13 any, T14 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, error) {
	var (
		r0  T0
		r1  T1
		r2  T2
		r3  T3
		r4  T4
		r5  T5
		r6  T6
		r7  T7
		r8  T8
		r9  T9
		r10 T10
		r11 T11
		r12 T12
		r13 T13
		r14 T14
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2, &r3, &r4, &r5, &r6, &r7, &r8, &r9, &r10, &r11, &r12, &r13, &r14)...)
	return r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, err
}

// Get16 is used to get the result of task / actor method with 16 return value(s).
// [WithTimeout]() can be used as options to set timeout.
func Get16[T0 any, T1 any, T2 any, T3 any, T4 any, T5 any, T6 any, T7 any, T8 any, T9 any, T10 any, T11 any, T12 any, T13 any, T14 any, T15 any](obj Decodable, options ...GetObjectOption) (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, error) {
	var (
		r0  T0
		r1  T1
		r2  T2
		r3  T3
		r4  T4
		r5  T5
		r6  T6
		r7  T7
		r8  T8
		r9  T9
		r10 T10
		r11 T11
		r12 T12
		r13 T13
		r14 T14
		r15 T15
	)
	err := obj.GetInto(appendOptions(options, &r0, &r1, &r2, &r3, &r4, &r5, &r6, &r7, &r8, &r9, &r10, &r11, &r12, &r13, &r14, &r15)...)
	return r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, err
}
