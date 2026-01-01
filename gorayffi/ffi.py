from __future__ import annotations

import ctypes
import logging
from ctypes.util import find_library
from typing import Callable


__all__ = [
    "load_go_lib",
]

logger = logging.getLogger(__name__)

# lib path -> GoCommander
_loaded_libs: dict[str, Callable[[int, int, bytes], tuple[bytes, int]]] = {}

# vars needed to put in global to prevent GC
_global_vars = []

cmdBitsLen = 10


def load_go_lib(
    libpath: str,
    handle_func: Callable[[int, bytes], tuple[bytes, int]],
) -> Callable[[int, int, bytes], tuple[bytes, int]]:
    """
    Load a go shared library and build a command-based protocol between python and golang.

    :param libpath: The path to the go shared library.
    :param handle_func: The function to handle the go commands.
        Signature: handle_func(cmd: int, data: bytes) -> tuple[bytes, int]
        - cmd: The command to handle.
        - data: The data to handle.
        return: The response to send to golang: (data, ret_code)
    :return: The function to send commands to golang.
        Signature: execute(cmd: int, index: int, data: bytes) -> tuple[bytes, int]
        - cmd: The command to send to golang.
        - index: extra int32 info to send to golang.
        - data: The data to send to golang.
        return: The response from golang: (data, ret_code)
    """
    if libpath in _loaded_libs:
        return _loaded_libs[libpath]

    go_lib = ctypes.CDLL(libpath)

    # C: void* Execute(long long cmd, void* in_data, long long data_len, long long* out_len, long long* ret_code)
    go_lib.Execute.argtypes = [
        ctypes.c_longlong,  # long long cmd
        ctypes.c_void_p,  # void* in_data
        ctypes.c_longlong,  # long long data_len
        ctypes.POINTER(ctypes.c_longlong),  # long long* out_len
        ctypes.POINTER(ctypes.c_longlong),  # long long* ret_code
    ]
    go_lib.Execute.restype = ctypes.c_void_p  # void*

    # 为 FreeMemory 函数定义原型
    # C: void FreeMemory(void* ptr)
    go_lib.FreeMemory.argtypes = [ctypes.c_void_p]
    go_lib.FreeMemory.restype = None  # void 返回值

    # 加载 C 标准库 (libc) 以便使用 malloc
    libc = ctypes.CDLL(find_library("c"))
    libc.malloc.argtypes = [ctypes.c_size_t]
    libc.malloc.restype = ctypes.c_void_p

    # 回调函数 C 签名: void* callback(long long cmd, void* in_data, long long in_len, long long* out_len, long long* ret_code)
    CALLBACK_TYPE = ctypes.CFUNCTYPE(
        ctypes.c_void_p,  # 返回类型: void*
        ctypes.c_longlong,  # 参数0: long long cmd
        ctypes.c_void_p,  # 参数1: void* in_data
        ctypes.c_longlong,  # 参数2: long long in_len
        ctypes.POINTER(ctypes.c_longlong),  # 参数3: long long* out_len
        ctypes.POINTER(ctypes.c_longlong),  # 参数4: long long* ret_code
    )

    register_callback_func = go_lib.RegisterCallback
    register_callback_func.argtypes = [CALLBACK_TYPE]
    register_callback_func.restype = None

    """
    GO call Python
    用于 Go2PyCmd_* 类型的请求，如 Go2PyCmd_ExeRemoteTask、Go2PyCmd_GetObject、Go2PyCmd_PutObject
    - 入参bytes, go分配，go回收，CallServer返回时生命周期结束
    - 返回值bytes, python分配，go回收，数据用完后生命周期结束
    """

    def _callback_wrapper(
        command: int,
        in_data_ptr: int,
        in_len: int,
        out_len_ptr: ctypes._Pointer[ctypes.c_longlong],
        ret_code_ptr: ctypes._Pointer[ctypes.c_longlong],
    ) -> ctypes.c_void_p:
        """
        这个函数将被 Go 调用。
        它接收数据，处理后，分配新内存返回结果。
        """
        received_data = c_pointer_to_bytes(in_data_ptr, in_len, zero_copy=True)

        response_bytes, ret_code = handle_func(command, received_data.tobytes())
        response_len = len(response_bytes)
        ret_code_ptr.contents.value = ret_code

        # 分配内存以存放返回数据
        # !! 关键: 必须使用 C 的 malloc 分配内存。
        # 这块内存的生命周期将由调用者(Go)管理。
        out_buffer_ptr = libc.malloc(response_len)
        if not out_buffer_ptr:
            # 内存分配失败
            logger.debug("Error: libc.malloc failed")
            # 通过设置 out_len 为 0 表示失败
            ret_code_ptr.contents.value = 0
            return ctypes.c_void_p(0)

        # 4. 将 Python bytes 数据复制到新分配的 C 内存中
        ctypes.memmove(out_buffer_ptr, response_bytes, response_len)

        # 5. 通过指针设置返回数据的长度
        # .contents 用于解引用指针
        out_len_ptr.contents.value = response_len
        return out_buffer_ptr

    # 将 Python 函数转换为 C 可调用的函数指针
    # 需要全局化，否则会被 GC 回收
    _c_callback = CALLBACK_TYPE(_callback_wrapper)
    _global_vars.append(_c_callback)

    register_callback_func(_c_callback)

    """
    Python call GO
    用于 Py2GoCmd_* 类型的请求，如 Py2GoCmd_RunTask、Py2GoCmd_NewActor、Py2GoCmd_ActorMethodCall
    
    C API: void* Execute(long long request, void* in_data, long long in_data_len, long long* out_len, long long* ret_code)
    - 入参bytes, python分配，python回收，Execute返回时生命周期结束
    - 返回值bytes, go分配，python回收，数据用完后生命周期结束
    """

    def execute(cmd: int, index: int, data: bytes) -> tuple[bytes, int]:
        request = cmd | index << cmdBitsLen

        out_len = ctypes.c_longlong(0)
        ret_code = ctypes.c_longlong(0)

        out_ptr = go_lib.Execute(
            request,
            data,  # 直接传入 bytes 对象, 零拷贝
            len(data),
            ctypes.byref(out_len),  # 使用 ctypes.byref() 来传递地址
            ctypes.byref(ret_code),
        )
        try:
            if not out_ptr:
                # go Execute return null pointer
                # todo: b'' or None?
                result_bytes = b""
            else:
                result_bytes = ctypes.string_at(out_ptr, out_len.value)
            return result_bytes, ret_code.value
        finally:
            # 关键步骤：调用 FreeMemory 释放 Go 中分配的 C 内存
            if out_ptr:
                go_lib.FreeMemory(out_ptr)

    _loaded_libs[libpath] = execute
    return _loaded_libs[libpath]


def c_pointer_to_bytes(c_ptr: int, length: int, zero_copy: bool) -> memoryview:
    """
    将 C 指针转换为 Python bytes
    """
    if not c_ptr or length <= 0:
        return memoryview(b"")
    if not zero_copy:
        return memoryview(ctypes.string_at(c_ptr, length))

    buffer_type = ctypes.c_char * length
    buffer_instance = buffer_type.from_address(c_ptr)
    memview = memoryview(buffer_instance)
    return memview
