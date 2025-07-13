import ctypes
import time
import os
import platform
from ctypes.util import find_library
import functools

# execute 需要在 register_handler 之后调用
__all__ = [
    'register_handler',
    'execute',
]

print(os.listdir('.'))
lib_path = "./lib/callback_demo.so"
go_lib = ctypes.CDLL(lib_path)

# C: void* Execute(long long cmd, void* in_data, long long data_len, long long* out_len, long long* ret_code)
go_lib.Execute.argtypes = [
    ctypes.c_longlong,      # long long cmd
    ctypes.c_void_p,        # void* in_data
    ctypes.c_longlong,      # long long data_len
    ctypes.POINTER(ctypes.c_longlong), # long long* out_len
    ctypes.POINTER(ctypes.c_longlong), # long long* ret_code
]
go_lib.Execute.restype = ctypes.c_void_p # void*

# 为 FreeMemory 函数定义原型
# C: void FreeMemory(void* ptr)
go_lib.FreeMemory.argtypes = [ctypes.c_void_p]
go_lib.FreeMemory.restype = None  # void 返回值

# 加载 C 标准库 (libc) 以便使用 malloc
libc = ctypes.CDLL(find_library('c'))
libc.malloc.argtypes = [ctypes.c_size_t]
libc.malloc.restype = ctypes.c_void_p

# 回调函数 C 签名: void* callback(long long cmd, void* in_data, long long in_len, long long* out_len, long long* ret_code)
CALLBACK_TYPE = ctypes.CFUNCTYPE(
    ctypes.c_void_p,  # 返回类型: void*
    ctypes.c_longlong, # 参数0: long long cmd
    ctypes.c_void_p,  # 参数1: void* in_data
    ctypes.c_longlong, # 参数2: long long in_len
    ctypes.POINTER(ctypes.c_longlong), # 参数3: long long* out_len
    ctypes.POINTER(ctypes.c_longlong), # 参数4: long long* ret_code
)

# ---  设置 Go 导出函数的原型 ---
register_callback_func = go_lib.ResigterCallback
register_callback_func.argtypes = [CALLBACK_TYPE]
register_callback_func.restype = None

_handle_func = None

# --- Python 回调函数 ---
def _callback_wrapper(cmd, in_data_ptr, in_len, out_len_ptr, ret_code_ptr):
    """
    这个函数将被 Go 调用。
    它接收数据，处理后，分配新内存返回结果。
    """
    # 1. 将输入的 C 指针和长度转换为 Python bytes
    # ctypes.string_at 从内存地址读取指定长度的字节
    received_data = ctypes.string_at(in_data_ptr, in_len)

    # 2. 处理数据
    response_bytes, ret_code = _handle_func(cmd, received_data)
    response_len = len(response_bytes)
    ret_code_ptr.contents.value = ret_code

    # 3. 分配内存以存放返回数据
    # !! 关键: 必须使用 C 的 malloc 分配内存。
    # 这块内存的生命周期将由调用者(Go)管理。
    out_buffer_ptr = libc.malloc(response_len)
    if not out_buffer_ptr:
        # 内存分配失败
        print("Error: libc.malloc failed")
        # 通过设置 out_len 为 0 表示失败
        ret_code_ptr.contents.value = 0
        return None

    # 4. 将 Python bytes 数据复制到新分配的 C 内存中
    ctypes.memmove(out_buffer_ptr, response_bytes, response_len)
    
    # 5. 通过指针设置返回数据的长度
    # .contents 用于解引用指针
    out_len_ptr.contents.value = response_len
    return out_buffer_ptr

# 将 Python 函数转换为 C 可调用的函数指针
# 需要全局化，否则会被 GC 回收
_c_callback = CALLBACK_TYPE(_callback_wrapper)
_has_register = False

def register_handler(handle_func):
    global _handle_func, _has_register
    if _has_register:
        return
    _handle_func = handle_func
    print("[py:ffi] register callback")
    register_callback_func(_c_callback)
    print("[py:ffi] register callback done")
    _has_register = True



def execute(cmd: int, data: bytes) -> tuple[bytes, int]:
    assert _has_register, "must register handler first"

    out_len = ctypes.c_longlong(0)
    ret_code = ctypes.c_longlong(0)
    # 调用函数
    # ctypes 会自动将 python bytes 转换为 c_char_p 或 c_void_p
    # 使用 ctypes.byref() 来传递 out_len 的指针
    out_ptr = go_lib.Execute(
        cmd,
        data,
        len(data),
        ctypes.byref(out_len),
        ctypes.byref(ret_code),
    )
    try:
        if not out_ptr:
            # go Execute return null pointer
            # todo: b'' or None?
            result_bytes = b''
        else:
            result_bytes = ctypes.string_at(out_ptr, out_len.value)
        return result_bytes, ret_code.value
    finally:
        # 关键步骤：调用 FreeMemory 释放 Go 中分配的 C 内存
        if out_ptr:
            go_lib.FreeMemory(out_ptr)


