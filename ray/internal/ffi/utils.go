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
		// C.CBytes will allocate memory in C heap and copy the data, so it must be freed manually later.
		cbytes := C.CBytes(data)
		return cbytes, len(data), func() {
			C.free(cbytes)
		}
	}
}
