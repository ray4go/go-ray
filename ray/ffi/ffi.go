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

	"github.com/ray4go/go-ray/ray/utils/log"
)

var (
	serverCallback C.ComplexCallbackFunc

	handler          func(int64, []byte) ([]byte, int64)
	setHandlerOnce   sync.Once                           // 确保值只被设置一次
	handlerReadyChan chan struct{} = make(chan struct{}) // 当值被设置后，向此通道发送信号

	ZeroCopy = true
)

/*
硬时序限制：
- 先 go:RegisterHandler 后 py:Execute
- 先 py:ResigterCallback 后 go:CallServer

保证：
由于要求用户程序在 init() 中调用 ray.Init(), 因而调用 RegisterHandler。而，python端需要再 ctypes.CDLL(golibpath) 后，才调用 Execute
OS保证 Execute 在 RegisterHandler 之后被调用。，

Go 端上层代码保证 CallServer 在 ResigterCallback 触发后再调用：
理论上，GO ray app的首次CallServer调用在go ray driver中，go ray driver由python端通过 Execute 触发，此时python端已经完成了 ResigterCallback。
*/

func RegisterHandler(handler_ func(int64, []byte) ([]byte, int64)) {
	setHandlerOnce.Do(func() {
		handler = handler_
		close(handlerReadyChan)
	})
}

//export ResigterCallback
func ResigterCallback(callback C.ComplexCallbackFunc) {
	log.Debug("[Go:ffi] Get callback\n")
	serverCallback = callback
}

/*
GO call Python
用于 Go2PyCmd_* 类型的请求，如 Go2PyCmd_ExeRemoteTask、Go2PyCmd_GetObject、Go2PyCmd_PutObject

C.invoke_callback:
- 入参bytes, go分配，go回收，CallServer返回时生命周期结束
- 返回值bytes, python分配，go回收，数据用完后生命周期结束
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
	cOutData := C.invoke_callback(serverCallback, C.longlong(request), cInData, cInLen, &cOutLen, &cRetCode)

	// !! Python/C 分配的内存，Go 负责释放
	// cOutData 指向由 Python 的 ctypes/libc.malloc 分配的内存,必须在使用后手动释放它。
	defer C.free(unsafe.Pointer(cOutData))

	// 将返回的 C 数据转换回 Go 的 slice
	// C.GoBytes 会创建一个新的 Go slice，并把 C 内存中的数据复制过来, 这样返回的 Go slice 就受 Go GC 管理了
	return C.GoBytes(cOutData, C.int(cOutLen)), int64(cRetCode)
}

/*
Python call GO
用于 Py2GoCmd_* 类型的请求，如 Py2GoCmd_RunTask、Py2GoCmd_NewActor、Py2GoCmd_ActorMethodCall

C API: void* Execute(long long request, void* in_data, long long in_data_len, long long* out_len, long long* ret_code)
- 入参bytes, python分配，python回收，Execute返回时生命周期结束
- 返回值bytes, go分配，python回收，数据用完后生命周期结束
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
		// 不能直接返回 Go slice 的内存指针
		// 因为 Go 的垃圾回收器(GC)可能会移动或回收这块内存，导致C/Python端出现悬挂指针
		// 必须在 C 的堆上分配内存，并将数据复制， C.malloc 分配的内存不受Go GC管理
		c_out_buf = C.malloc(C.size_t(resultLen))
		if c_out_buf == nil {
			// 内存分配失败
			resultLen = 0
			// todo: log error when origin errorCode != 0
			errorCode = 1
		} else {
			// 将 Go slice 的数据复制到 C 分配的内存中
			// C.memcpy(destination, source, size)
			C.memcpy(c_out_buf, unsafe.Pointer(&resultBytes[0]), C.size_t(resultLen))
		}
	}
	// 通过出参指针设置返回数据的长度
	*out_len = C.longlong(resultLen)
	*ret_code = C.longlong(errorCode)
	return c_out_buf
}

// FreeMemory 释放由 Execute 函数分配的内存
//
// C函数签名:
// void FreeMemory(void* ptr)
//
//export FreeMemory
func FreeMemory(ptr unsafe.Pointer) {
	// fmt.Println("[Go] Freeing memory...")
	C.free(ptr)
}
