import io
import threading
import traceback
import time
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
import sys
import argparse

from . import libpath, utils


cmdBitsLen = 10
cmdBitsMask = (1 << cmdBitsLen) - 1

logger = logging.getLogger(__name__)
utils.init_logger(logger)

# 和 go 中的 enum 对应
class Go2PyCmd(enum.IntEnum):
    CMD_INIT = 0
    CMD_EXECUTE_REMOTE_TASK = 1
    CMD_GET_OBJECTS = 2
    CMD_EXECUTE_PYTHON_CODE = 3

class Py2GoCmd(enum.IntEnum):
    CMD_START_DRIVER = 0
    CMD_RUN_TASK = 1


@ray.remote
def ray_run_task(func_id: int, data: bytes) -> tuple[bytes, int]:
    global _has_init

    _has_init = globals().get("_has_init", False)
    if not _has_init:
        from . import ffi
        _has_init = True
        ffi.register_handler(functools.partial(handle, ray_handlers))
        logger.debug("register handler")
        
    return local_run_task(func_id, data)

@ray.remote
def show(data):
    # debug
    logger.debug('='*10, data)

def local_run_task(func_id: int, data: bytes) -> tuple[bytes, int]:
    from . import ffi
    cmd = Py2GoCmd.CMD_RUN_TASK | func_id << cmdBitsLen
    try:
        res, code = ffi.execute(cmd, data)
        logger.debug(f"[py] local_run_task {func_id=}, {res=} {code=}")
    except Exception as e:
        logging.exception(f"[py] execute error {e}")
        return f"[goray error] python ffi.execute() error: {e}".encode('utf-8'), 1
    return res, code


def handle_init(data: bytes, _: int) -> tuple[bytes, int]:
    logger.debug(f"[py] init")
    return b'', 0

def handle_run_python_code(code: bytes, _: int) -> tuple[bytes, int]:
    stream = io.StringIO()
    injects = {
        'put': lambda x: stream.write(str(x)),
    }
    try:
        """
        Remember that at module level, globals and locals are the same dictionary. 
        If exec gets two separate objects as globals and locals, 
        the code will be executed as if it were embedded in a class definition.
        https://docs.python.org/3/library/functions.html#exec
        """
        exec(code.decode('utf-8'), injects)
    except Exception as e:
        return f"[goray error] python exec() error: {e}".encode('utf-8'), 1
    
    return stream.getvalue().encode("utf-8"), 0


_futures = {}

def handle_run_remote_task(data: bytes, bitmap: int) -> tuple[bytes, int]:
    func_id = bitmap & ((1 << 22) - 1)
    option_len = bitmap >> 22
    options = json.loads(data[-option_len:])
    logger.debug(f"[py] run remote task {func_id}, {options=}")
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
    logger.debug(f"[Py] get obj {obj_ref.hex()}")
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
    Go2PyCmd.CMD_EXECUTE_PYTHON_CODE: handle_run_python_code,
}
local_handlers = {
    Go2PyCmd.CMD_INIT: handle_init,
    Go2PyCmd.CMD_EXECUTE_REMOTE_TASK: handle_run_local_task,
    Go2PyCmd.CMD_GET_OBJECTS: handle_get_local_objects,
    Go2PyCmd.CMD_EXECUTE_PYTHON_CODE: handle_run_python_code,
}

def handle(hanlers, cmd: int, data: bytes) -> tuple[bytes, int]:
    cmd, index = cmd & cmdBitsMask, cmd >> cmdBitsLen
    logger.debug(f"[py] handle {Go2PyCmd(cmd).name}, {index=}, {len(data)=}, {threading.current_thread().name}")
    func = hanlers[cmd]
    try:
        return func(data, index)
    except Exception as e:
        logging.exception(f"[py] handle {Go2PyCmd(cmd).name} error {e}")
        return b'', 0


def main():
    parser = argparse.ArgumentParser(
        description="Python driver for ray-core-go application.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--debug', action='store_true', help="Enable debug logging")
    parser.add_argument(
        '--mode',
        type=str,
        choices=['cluster', 'local', 'mock'],
        default='cluster',
        help="指定运行模式：\n"
             "  cluster: 在集群模式下运行\n"
             "  local: 在ray本地模式下运行\n"
             "  mock: 在模拟模式下运行"
    )
    parser.add_argument(
        'go_binary_path',  # 位置参数
        type=str,
        help="指定 Goray 应用二进制文件的路径 (使用 go build -buildmode=c-shared 构建)"
    )
    args = parser.parse_args()
    
    libpath.golibpath = args.go_binary_path
    from . import ffi

    ray_runtime_env = {"env_vars": {"GORAY_BIN_PATH": args.go_binary_path}}
    if args.debug:
        ray_runtime_env["env_vars"]["GORAY_DEBUG_LOGGING"] = "1"
        utils.enable_logger(logger)
        utils.enable_logger(ffi.logger)

    if args.mode == 'cluster':
        ray.init(address='auto', runtime_env=ray_runtime_env)
        handlers = ray_handlers
    elif args.mode == 'local':
        ray.init(runtime_env=ray_runtime_env)
        handlers = ray_handlers
    elif args.mode == 'mock':
        handlers = local_handlers

    ffi.register_handler(functools.partial(handle, handlers))

    try:
        data, code = ffi.execute(Py2GoCmd.CMD_START_DRIVER, b'')
        if code != 0:
            err_msg = data.decode('utf-8')
            logger.debug(f"[py] driver error[{code}]: {err_msg}")
            exit(1)
    except KeyboardInterrupt:
        logger.debug("Exiting...")


if __name__ == "__main__":
    main()

