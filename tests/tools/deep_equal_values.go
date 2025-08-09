package tools

import (
	"math"
	"reflect"
	"unsafe"
)

// DeepEqualValues 比较两个变量 a 和 b 是否在“值”的意义上相等。
//
// 与 reflect.DeepEqual 的不同之处在于：
//  1. 忽略数值类型差异：只要数值相等，不同类型的整数（如 int, int8, int64）之间被视为相等。
//     浮点数（float32, float64）同理。
//  2. 忽略容器类型差异：只要内容的值相等，不同类型的切片或映射（如 []int vs []any,
//     map[string]int vs map[any]any）也被视为相等。
//  3. 特殊规则：整数和浮点数永远不相等，即使数值上相等（例如 1 != 1.0）。
//  4. 边界情况：nil 切片/映射与同类型的空切片/映射被视作相等。
func DeepEqualValues(a, b any) bool {
	v1 := reflect.ValueOf(a)
	v2 := reflect.ValueOf(b)
	// 使用一个 map 来跟踪已经比较过的指针，以防止无限递归（循环引用）
	visited := make(map[visit]bool)
	return deepValueEqual(v1, v2, visited)
}

// visit 结构用于在 visited map 中唯一标识一对正在被比较的值。
// 我们使用值的地址和类型来作为键。
type visit struct {
	a1  unsafe.Pointer
	a2  unsafe.Pointer
	typ reflect.Type
}

// deepValueEqual是实现深度值比较的核心递归函数。
func deepValueEqual(v1, v2 reflect.Value, visited map[visit]bool) bool {
	// 1. 处理接口类型
	// 在进行任何比较之前，首先“解包”接口，获取其内部包含的具体值。
	if v1.Kind() == reflect.Interface {
		if v1.IsNil() { // 如果接口本身为 nil
			// 只有当另一个值也是 nil 接口时才相等
			return v2.Kind() == reflect.Interface && v2.IsNil()
		}
		v1 = v1.Elem()
	}
	if v2.Kind() == reflect.Interface {
		if v2.IsNil() {
			// v1 此时必然不是 nil 接口（在上面已处理），所以不相等
			return false
		}
		v2 = v2.Elem()
	}

	// 2. 有效性检查
	// 如果解包后值无效（例如，来自一个 untyped nil），则只有在两者都无效时才相等。
	if !v1.IsValid() || !v2.IsValid() {
		return v1.IsValid() == v2.IsValid()
	}

	// 3. 类型种类兼容性检查
	// 这是实现规则的关键：不同种类的类型（如 int vs float, slice vs map）不相等。
	k1, k2 := v1.Kind(), v2.Kind()
	if !areKindsCompatible(k1, k2) {
		return false
	}

	// 4. 循环引用检查
	// 对于可能导致循环的类型（指针、切片、映射），检查是否已经访问过。
	if v1.CanAddr() && v2.CanAddr() {
		addr1 := unsafe.Pointer(v1.UnsafeAddr())
		addr2 := unsafe.Pointer(v2.UnsafeAddr())
		if addr1 == addr2 {
			return true // 指向同一块内存，必然相等
		}
		v := visit{addr1, addr2, v1.Type()}
		if visited[v] {
			return true // 已访问过，说明是循环，并且到目前为止都相等
		}
		visited[v] = true
	}

	// 5. 按类型种类进行分发比较
	switch k1 {
	case reflect.Bool:
		return v1.Bool() == v2.Bool()

	case reflect.String:
		return v1.String() == v2.String()

	// 整数和浮点数家族
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fallthrough
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return compareInts(v1, v2)

	case reflect.Float32, reflect.Float64:
		return math.Abs(v1.Float()-v2.Float()) < 1e-6

	// 切片比较
	case reflect.Slice:
		// 将 nil 切片视为空切片
		isNil1, len1 := v1.IsNil(), v1.Len()
		isNil2, len2 := v2.IsNil(), v2.Len()
		if (isNil1 || len1 == 0) && (isNil2 || len2 == 0) {
			return true
		}
		if len1 != len2 {
			return false
		}
		for i := 0; i < len1; i++ {
			if !deepValueEqual(v1.Index(i), v2.Index(i), visited) {
				return false
			}
		}
		return true

	// 映射比较
	case reflect.Map:
		// 将 nil 映射视为空映射
		isNil1, len1 := v1.IsNil(), v1.Len()
		isNil2, len2 := v2.IsNil(), v2.Len()
		if (isNil1 || len1 == 0) && (isNil2 || len2 == 0) {
			return true
		}
		if len1 != len2 {
			return false
		}

		keys1 := v1.MapKeys()
		keys2 := v2.MapKeys()
		used := make([]bool, len2)

		// O(n^2) 检查：对于 map1 中的每个键，在 map2 中寻找一个值相等的键
		for _, k1 := range keys1 {
			foundMatch := false
			for j, k2 := range keys2 {
				if used[j] {
					continue
				}
				if deepValueEqual(k1, k2, visited) {
					val1 := v1.MapIndex(k1)
					val2 := v2.MapIndex(k2)
					if deepValueEqual(val1, val2, visited) {
						used[j] = true
						foundMatch = true
						break
					}
				}
			}
			if !foundMatch {
				return false
			}
		}
		return true
	}

	// 对于未处理的类型（如 chan, func, struct 等），我们认为它们不相等。
	return false
}

// areKindsCompatible 检查两个 Kind 是否可以进行值比较。
func areKindsCompatible(k1, k2 reflect.Kind) bool {
	// 基础类型必须属于同一个“家族”
	isK1Int := (k1 >= reflect.Int && k1 <= reflect.Int64) || (k1 >= reflect.Uint && k1 <= reflect.Uintptr)
	isK2Int := (k2 >= reflect.Int && k2 <= reflect.Int64) || (k2 >= reflect.Uint && k2 <= reflect.Uintptr)
	if isK1Int && isK2Int {
		return true
	}

	isK1Float := k1 >= reflect.Float32 && k1 <= reflect.Float64
	isK2Float := k2 >= reflect.Float32 && k2 <= reflect.Float64
	if isK1Float && isK2Float {
		return true
	}

	// 对于容器和布尔/字符串类型，它们的 Kind 必须完全相同。
	if k1 == k2 {
		return k1 == reflect.Slice || k1 == reflect.Map || k1 == reflect.Bool || k1 == reflect.String
	}

	return false
}

// compareInts 比较两个整数 reflect.Value，可以处理有符号和无符号整数的混合比较。
func compareInts(v1, v2 reflect.Value) bool {
	k1, k2 := v1.Kind(), v2.Kind()
	isV1Signed := k1 >= reflect.Int && k1 <= reflect.Int64
	isV2Signed := k2 >= reflect.Int && k2 <= reflect.Int64

	switch {
	case isV1Signed && isV2Signed:
		return v1.Int() == v2.Int()
	case !isV1Signed && !isV2Signed:
		return v1.Uint() == v2.Uint()
	case isV1Signed && !isV2Signed: // v1有符号, v2无符号
		i1 := v1.Int()
		u2 := v2.Uint()
		if i1 < 0 {
			return false
		}
		return uint64(i1) == u2
	case !isV1Signed && isV2Signed: // v1无符号, v2有符号
		u1 := v1.Uint()
		i2 := v2.Int()
		if i2 < 0 {
			return false
		}
		return u1 == uint64(i2)
	}
	return false // 不应到达这里
}
