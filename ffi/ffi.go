package ffi

/*
#include <stdlib.h> // 为了 C.malloc 和 C.free
#include <string.h> // 为了 C.memcpy

// 定义新的回调函数C指针类型
typedef void* (*ComplexCallbackFunc)(long long cmd, void* in_data, long long in_len, long long* out_len);

// C 辅助函数 (trampoline) 现在也需要处理参数和返回值
static void* invoke_callback(
    ComplexCallbackFunc cb,
	long long cmd,
    void* in_data,
    long long in_len,
    long long* out_len)
{
    // 执行回调并返回结果
    return cb(cmd, in_data, in_len, out_len);
}
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"
)

var (
	serverCallback C.ComplexCallbackFunc

	handler func(int64, []byte) []byte
	// 可能没必要，如果上层在init中RegisterHandler的话
	setHandlerOnce   sync.Once                           // 确保值只被设置一次
	handlerReadyChan chan struct{} = make(chan struct{}) // 当值被设置后，向此通道发送信号
)

/*
硬时序限制：
- 先 ResigterCallback 后 CallServer
- 先 RegisterHandler 后 Execute

保证：
由 Python 端保证 Execute 在 ResigterCallback 之后被调用。
Go 端上层代码需要保证 CallServer 在 ResigterCallback 触发后再调用。否则panic。
本模块保证 RegisterHandler 在 Execute 之前调用。
*/

func RegisterHandler(handler_ func(int64, []byte) []byte) {
	setHandlerOnce.Do(func() {
		handler = handler_
		close(handlerReadyChan)
	})
}

//export ResigterCallback
func ResigterCallback(callback C.ComplexCallbackFunc) {
	fmt.Printf("[Go:ffi] Get callback\n")
	serverCallback = callback
}

func CallServer(cmd int64, data []byte) []byte {
	fmt.Printf("[Go:ffi] CallServer cmd: %v, len(data)=%d\n", cmd, len(data))
	// 1. 确保回调函数已注册
	if serverCallback == nil {
		panic("Callback function not registered")
	}

	// 使用 C.CBytes 将 Go slice 转换为 C 的 void*。
	// 这会在 C 的堆上分配内存并复制数据。
	// 我们必须在之后手动释放它。
	cInData := C.CBytes(data)
	defer C.free(cInData) // 确保在函数退出时释放内存

	cInLen := C.longlong(len(data))
	var cOutLen C.longlong
	cOutData := C.invoke_callback(serverCallback, C.longlong(cmd), cInData, cInLen, &cOutLen)

	// !! 关键：Python/C 分配的内存，Go 负责释放
	// cOutData 指向由 Python 的 ctypes/libc.malloc 分配的内存
	// 我们必须在使用后手动释放它。
	defer C.free(unsafe.Pointer(cOutData))

	// 4. 将返回的 C 数据转换回 Go 的 slice
	// C.GoBytes 会创建一个新的 Go slice，并把 C 内存中的数据复制过来。
	// 这样返回的 goReturnData 就是受 Go GC 管理的安全数据了。
	return C.GoBytes(cOutData, C.int(cOutLen))
}

// Execute 是我们将要导出的函数。
// 1. 使用 //export Execute 注释来告诉 cgo 导出这个函数。
// 2. Go 函数的参数类型需要对应 C 类型。
// 3. 返回值是一个指向C堆内存的指针。内存由调用者负责释放。
//
// C函数签名:
// void* Execute(long long cmd, void* in_data, long long in_data_len, long long* out_len)
//
//export Execute
func Execute(cmd C.longlong, in_data unsafe.Pointer, data_len C.longlong, out_len *C.longlong) unsafe.Pointer {
	fmt.Printf("[Go:ffi] Execute cmd: %v\n", cmd)
	if handler == nil {
		<-handlerReadyChan // wait for handler to be set
	}

	// C.GoBytes 是安全的，因为它处理了指定长度的 void* 数据
	goInData := C.GoBytes(in_data, C.int(data_len))
	resultBytes := handler(int64(cmd), goInData)
	resultLen := len(resultBytes)
	// --- 3. 准备返回值 ---
	// 重要：不能直接返回 Go slice 的内存指针！
	// 因为 Go 的垃圾回收器(GC)可能会移动或回收这块内存，导致C/Python端出现悬挂指针。
	// 我们必须在 C 的堆上分配内存，并将数据复制过去。
	// C.malloc 分配的内存不受Go GC管理。
	c_out_buf := C.malloc(C.size_t(resultLen))
	if c_out_buf == nil {
		// 内存分配失败
		*out_len = 0
		return nil
	}

	if resultLen > 0 {
		// 将 Go slice 的数据复制到 C 分配的内存中
		// C.memcpy(destination, source, size)
		C.memcpy(c_out_buf, unsafe.Pointer(&resultBytes[0]), C.size_t(resultLen))
	}
	// 通过出参指针设置返回数据的长度
	*out_len = C.longlong(resultLen)
	// 返回指向 C 堆内存的指针
	return c_out_buf
}

// FreeMemory 导出一个函数，用于释放由 Execute 函数分配的内存。
// 这非常重要，可以防止内存泄漏。调用者（Python）有责任调用此函数。
//
// C函数签名:
// void FreeMemory(void* ptr)
//
//export FreeMemory
func FreeMemory(ptr unsafe.Pointer) {
	// fmt.Println("[Go] Freeing memory...")
	C.free(ptr)
}

// func main() {}
