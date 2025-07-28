package ffi

//#include <stdlib.h>
import "C"
import "unsafe"

func cPointerToGoBytes(ptr unsafe.Pointer, length int, zeroCopy bool) []byte {
	var bytes []byte
	if zeroCopy {
		bytes = unsafe.Slice((*byte)(ptr), C.int(length))
	} else {
		bytes = C.GoBytes(ptr, C.int(length))
	}
	return bytes
}

func goBytesToCPointer(data []byte, zeroCopy bool) (unsafe.Pointer, int, func()) {
	if len(data) == 0 {
		return nil, 0, func() {}
	}
	if zeroCopy {
		return unsafe.Pointer(&data[0]), len(data), func() {}
	} else {
		// 使用 C.CBytes 将 Go slice 转换为 C 的 void*。
		// 这会在 C 的堆上分配内存并复制数据, 必须在之后手动释放它。
		cbytes := C.CBytes(data)
		return cbytes, len(data), func() {
			C.free(cbytes)
		}
	}
}
