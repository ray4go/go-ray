import gc
# 获取当前的GC阈值
# (threshold0, threshold1, threshold2)
# 设置一个非常激进的阈值来进行测试
# 当(分配次数 - 释放次数) > threshold0 时，触发gc0
# 我们把它设为1，让GC变得非常敏感
gc.set_threshold(1, 1, 1) 

import threading
import traceback
import time
import ffi
import ray
import os
import threading
import logging
import time
import datetime
import socket
import enum
import functools
import json

cmdBitsLen = 10
cmdBitsMask = (1 << cmdBitsLen) - 1

# 和 go 中的 enum 对应
class Go2PyCmd(enum.IntEnum):
    CMD_INIT = 0
    CMD_EXECUTE_REMOTE_TASK = 1
    CMD_GET_OBJECTS = 2

class Py2GoCmd(enum.IntEnum):
    CMD_START_DRIVER = 0
    CMD_RUN_TASK = 1


@ray.remote
def ray_run_task(func_id: int, data: bytes) -> bytes:
    global _has_init
    _has_init = globals().get("_has_init", False)
    if not _has_init:
        _has_init = True
        ffi.register_handler(functools.partial(handle, ray_handlers))
        print("register handler")
        
    return local_run_task(func_id, data)

def local_run_task(func_id: int, data: bytes) -> bytes:
    cmd = Py2GoCmd.CMD_RUN_TASK | func_id << cmdBitsLen
    try:
        res = ffi.execute(cmd, data)
    except Exception as e:
        logging.exception(f"[py] execute error {e}")
        return b''
    return res


def handle_init(data: bytes, extra: int) -> bytes:
    print(f"[py] init")
    return b''

def handle_run_remote_task(data: bytes, func_id: int) -> bytes:
    fut = ray_run_task.remote(func_id, data)
    return fut.hex().encode('utf-8') 

futures = {}
def handle_run_local_task(data: bytes, func_id: int) -> bytes:
    data = local_run_task(func_id, data)
    fut = len(futures)
    futures[fut] = data
    return str(fut).encode('utf-8') 


def handle_get_objects(data: bytes, extra: int) -> bytes:
    object_ids = data.decode('utf-8').split(',')
    fs = [
        ray._raylet.ObjectRef(bytes.fromhex(fut))
        for fut in object_ids
    ]
    res = ray.get(fs)
    return b''.join(res)


def handle_get_local_objects(data: bytes, index: int) -> bytes:
    res = [futures[int(i)] for i in data.decode('utf-8').split(',')]
    return b''.join(res)


ray_handlers = {
    Go2PyCmd.CMD_INIT: handle_init,
    Go2PyCmd.CMD_EXECUTE_REMOTE_TASK: handle_run_remote_task,
    Go2PyCmd.CMD_GET_OBJECTS: handle_get_objects,
}
local_handlers = {
    Go2PyCmd.CMD_INIT: handle_init,
    Go2PyCmd.CMD_EXECUTE_REMOTE_TASK: handle_run_local_task,
    Go2PyCmd.CMD_GET_OBJECTS: handle_get_local_objects,
}

def handle(hanlers, cmd: int, data: bytes) -> bytes:
    cmd, index = cmd & cmdBitsMask, cmd >> cmdBitsLen
    print(f"[py] handle {Go2PyCmd(cmd).name}, {index=}, {len(data)=}, {threading.current_thread().name}")
    func = hanlers[cmd]
    try:
        return func(data, index)
    except Exception as e:
        logging.exception(f"[py] handle {Go2PyCmd(cmd).name} error {e}")
        return b''


if __name__ == "__main__":
    import sys

    if 'local' == sys.argv[-1]:
        ffi.register_handler(functools.partial(handle, local_handlers))
    else:
        ray.init(address='auto')
        ffi.register_handler(functools.partial(handle, ray_handlers))

    try:
        ffi.execute(Py2GoCmd.CMD_START_DRIVER, b'')
    except KeyboardInterrupt:
        print("Exiting...")

