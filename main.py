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
import pickle

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
def ray_run_task(func_id: int, data: bytes) -> tuple[bytes, int]:
    global _has_init
    _has_init = globals().get("_has_init", False)
    if not _has_init:
        _has_init = True
        ffi.register_handler(functools.partial(handle, ray_handlers))
        print("register handler")
        
    return local_run_task(func_id, data)

@ray.remote
def show(data):
    # debug
    print('='*10, data)

def local_run_task(func_id: int, data: bytes) -> tuple[bytes, int]:
    cmd = Py2GoCmd.CMD_RUN_TASK | func_id << cmdBitsLen
    try:
        res, code = ffi.execute(cmd, data)
        print(f"[py] local_run_task {func_id=}, {res=} {code=}")
    except Exception as e:
        logging.exception(f"[py] execute error {e}")
        return f"[goray error] python ffi.execute() error: {e}".encode('utf-8'), 1
    return res, code


def handle_init(data: bytes, extra: int) -> tuple[bytes, int]:
    print(f"[py] init")
    return b'', 0


_futures = {}

def handle_run_remote_task(data: bytes, bitmap: int) -> tuple[bytes, int]:
    func_id = bitmap & ((1 << 22) - 1)
    option_len = bitmap >> 22
    options = json.loads(data[-option_len:])
    print(f"[py] run remote task {func_id}, {options=}")
    fut = ray_run_task.options(**options).remote(func_id, data[:-option_len])
    _futures[fut.hex()] = fut  # make future outlive this function
    args = dict(id=fut.binary(), owner_addr=fut.owner_address(), call_site_data=fut.call_site())
    return pickle.dumps(args), 0


def handle_get_objects(data: bytes, _: int) -> tuple[bytes, int]:
    # ray._raylet.ObjectRef(id:bytes, owner_addr=u'', call_site_data=u'', skip_adding_local_ref=False)
    obj_args = pickle.loads(data)
    obj_ref = ray._raylet.ObjectRef(**obj_args)
    if obj_ref.hex() in _futures:
        obj_ref = _futures[obj_ref.hex()]  # seems not necessary
    # show.remote(obj_ref)
    print(f"[Py] get obj {obj_ref.hex()}")
    res, code = ray.get(obj_ref)
    return res, code


futures:dict[int, tuple[bytes, int]] = {}
def handle_run_local_task(data: bytes, bitmap: int) -> tuple[bytes, int]:
    func_id = bitmap & ((1 << 22) - 1)
    option_len = bitmap >> 22
    ret = local_run_task(func_id, data[:-option_len])
    fut = len(futures)
    futures[fut] = ret
    return str(fut).encode('utf-8'), 0


def handle_get_local_objects(data: bytes, index: int) -> tuple[bytes, int]:
    future_id = int(data.decode('utf-8'))
    return futures[future_id]


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

def handle(hanlers, cmd: int, data: bytes) -> tuple[bytes, int]:
    cmd, index = cmd & cmdBitsMask, cmd >> cmdBitsLen
    print(f"[py] handle {Go2PyCmd(cmd).name}, {index=}, {len(data)=}, {threading.current_thread().name}")
    func = hanlers[cmd]
    try:
        return func(data, index)
    except Exception as e:
        logging.exception(f"[py] handle {Go2PyCmd(cmd).name} error {e}")
        return b'', 0


if __name__ == "__main__":
    import sys

    if 'local' == sys.argv[-1]:
        ffi.register_handler(functools.partial(handle, local_handlers))
    elif 'local-ray' == sys.argv[-1]:
        ray.init()
        ffi.register_handler(functools.partial(handle, ray_handlers))
    else:
        ray.init(address='auto')
        ffi.register_handler(functools.partial(handle, ray_handlers))

    try:
        data, code = ffi.execute(Py2GoCmd.CMD_START_DRIVER, b'')
        if code != 0:
            err_msg = data.decode('utf-8')
            print(f"[py] driver error[{code}]: {err_msg}")
            exit(1)
    except KeyboardInterrupt:
        print("Exiting...")

