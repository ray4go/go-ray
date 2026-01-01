package ffi

/*
#include <stdlib.h> // C.malloc && C.free
#include <string.h> // C.memcpy

// callback type in C
typedef void* (*ComplexCallbackFunc)(long long request, void* in_data, long long in_len, long long* out_len, long long* ret_code);

// called by Go
static void* invoke_callback(
    ComplexCallbackFunc cb,
	long long request,
    void* in_data,
    long long in_len,
    long long* out_len,
    long long* ret_code)
{
    return cb(request, in_data, in_len, out_len, ret_code);
}
*/
import "C"

import (
	"sync"
	"unsafe"

	"github.com/ray4go/go-ray/ray/internal/log"
)

var (
	serverCallback C.ComplexCallbackFunc

	handler          func(int64, []byte) ([]byte, int64)
	setHandlerOnce   sync.Once             // ensure handler is set only once
	handlerReadyChan = make(chan struct{}) // signal sent to this channel when the value is set

	ZeroCopy = true
)

/*
Hard timing constraints:
- First go:RegisterHandler then py:Execute
- First py:RegisterCallback then go:CallServer

How to ensure the order?

When python load go lib via ctypes.CDLL(golibpath), the Go runtime starts, go:init() is called synchronously,
so go:RegisterHandler is called. py:Execute will be called after ctypes.CDLL(golibpath).

The python side will call RegisterCallback then Execute(start_driver). All go:CallServer calls are triggered by py:Execute(start_driver).
*/

func RegisterHandler(handler_ func(command int64, data []byte) (res []byte, code int64)) {
	setHandlerOnce.Do(func() {
		handler = handler_
		close(handlerReadyChan)
	})
}

//export RegisterCallback
func RegisterCallback(callback C.ComplexCallbackFunc) {
	if serverCallback != nil {
		panic("[go:ffi] ServerCallback function already registered")
	}
	serverCallback = callback
}

/*
GO call Python.
Send Go2PyCmd_* commands to Python, such as Go2PyCmd_ExeRemoteTask, Go2PyCmd_GetObject, Go2PyCmd_PutObject
*/
func CallServer(request int64, data []byte) ([]byte, int64) {
	log.Debug("[Go:ffi] CallServer cmd: %v, len(data)=%d\n", request, len(data))
	if serverCallback == nil {
		panic("Callback function not registered")
	}
	cInData, dataLen, freeFunc := goBytesToCPointer(data, ZeroCopy)
	defer freeFunc()
	cInLen := C.longlong(dataLen)

	var cOutLen C.longlong
	var cRetCode C.longlong
	// cInData: allocate by Go, free by Go after CallServer returns
	// cOutData: allocate by Python, free by Go after the data is no longer needed
	cOutData := C.invoke_callback(serverCallback, C.longlong(request), cInData, cInLen, &cOutLen, &cRetCode)

	// cOutData is allocate by Python via ctypes/libc.malloc allocated memory, must be manually freed after use.
	defer C.free(unsafe.Pointer(cOutData))

	// C.GoBytes will copy data from C memory to a new Go slice, so the returned Go slice is managed by Go GC
	return C.GoBytes(cOutData, C.int(cOutLen)), int64(cRetCode)
}

/*
Python call GO

Send Py2GoCmd_* commands to Go. such as Py2GoCmd_RunTask, Py2GoCmd_NewActor, Py2GoCmd_ActorMethodCall

C API: void* Execute(long long request, void* in_data, long long in_data_len, long long* out_len, long long* ret_code)
- in_data: allocate by python, freed by python after Execute returns
- return bytes: allocate by go, freed by python after data is no longer needed
*/
//export Execute
func Execute(request C.longlong, in_data unsafe.Pointer, data_len C.longlong, out_len *C.longlong, ret_code *C.longlong) unsafe.Pointer {
	if handler == nil {
		<-handlerReadyChan // wait for handler to be set
	}
	log.Debug("[Go:ffi] Execute cmd: %v\n", request)
	goInData := cPointerToGoBytes(in_data, int(data_len), ZeroCopy)
	resultBytes, errorCode := handler(int64(request), goInData)
	resultLen := len(resultBytes)
	log.Debug("[Go:ffi] Execute result: %v, len=%v, errorCode=%v\n", resultBytes, resultLen, errorCode)

	var c_out_buf unsafe.Pointer = nil
	if resultLen > 0 {
		// we cannot directly return the memory pointer of Go slice
		// because Go's garbage collector (GC) may move or reclaim that memory, leading to dangling pointers on the C/Python side
		// we must allocate memory on C heap and copy the data there, since memory allocated by C.malloc is not managed by Go GC
		c_out_buf = C.malloc(C.size_t(resultLen))
		if c_out_buf == nil {
			// memory allocation failed
			resultLen = 0
			// todo: log error when origin errorCode != 0
			errorCode = 1
		} else {
			// copy Go slice data to C allocated memory
			// C.memcpy(destination, source, size)
			C.memcpy(c_out_buf, unsafe.Pointer(&resultBytes[0]), C.size_t(resultLen))
		}
	}
	// set the length of the returned data through the out parameter pointer
	*out_len = C.longlong(resultLen)
	*ret_code = C.longlong(errorCode)
	return c_out_buf
}

// FreeMemory frees the memory allocated by the Execute function
//
// C function signature:
// void FreeMemory(void* ptr)
//
//export FreeMemory
func FreeMemory(ptr unsafe.Pointer) {
	C.free(ptr)
}
