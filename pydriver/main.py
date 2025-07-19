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
def show(data):
    # debug
    print('='*10, data)

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




def init_ffi_once():
    global _has_init

    _has_init = globals().get("_has_init", False)
    if not _has_init:
        from . import ffi  # lazy import, since the libpath is injected dynamic 
        _has_init = True
        ffi.register_handler(functools.partial(handle, handlers))
        logger.debug("register handler")

@ray.remote
def ray_run_task(func_id: int, raw_args: bytes, object_positions: list[int], *object_refs:list[tuple[bytes, int]]) -> tuple[bytes, int]:
    """
    :param func_id:
    :param data:
    :param object_positions: 调用的go task func中, 传入的object_ref作为参数的位置列表, 有序
    :param object_refs: object_ref列表, 会被ray core替换为实际的数据
    :return:
    """
    init_ffi_once()
    return run_task(func_id, raw_args, object_positions, *object_refs)


def run_task(func_id: int, raw_args: bytes, object_positions: list[int], *object_refs:list[tuple[bytes, int]]) -> tuple[bytes, int]:
    data = [raw_args]
    for pos in object_positions:
        raw_res, code = object_refs[pos]
        if code != 0:
            origin_err_msg = raw_res.decode('utf-8')
            err_msg = f"get object for argument {pos} error: {origin_err_msg}"
            return err_msg.encode('utf-8'), code
        data.append(pos.to_bytes(8, byteorder='little') + raw_res)

    return local_run_task(func_id, pack_bytes_list(data))


def pack_bytes_list(data: list[bytes]) -> bytes:
    return b''.join([len(d).to_bytes(8, byteorder='little') + d for d in data])


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


# in mock mode, value is actual return value
# in other modes, value is ray object ref
_futures = {}

def handle_run_remote_task(data: bytes, bitmap: int, mock=False) -> tuple[bytes, int]:
    func_id = bitmap & ((1 << 22) - 1)
    option_len = bitmap >> 22
    args_data, opts_data = data[:-option_len], data[-option_len:]
    options = json.loads(opts_data)
    object_pos_to_local_id = options.pop("go_ray_object_pos_to_local_id", {})  
    logger.debug(f"[py] run remote task {func_id}, {options=}, {object_pos_to_local_id=}")

    object_positions = []
    object_refs = []
    for pos, local_id in sorted(object_pos_to_local_id.items()):
        object_positions.append(int(pos))
        if local_id not in _futures:
            return b"object_ref not found!", 1
        object_refs.append(_futures[local_id])
    
    if mock:
        fut = run_task(func_id, args_data, object_positions, *object_refs)
    else:
        fut = ray_run_task.options(**options).remote(func_id, args_data, object_positions, *object_refs)
    fut_local_id = len(_futures)
    _futures[fut_local_id] = fut  # make future outlive this function
    # show.remote(fut)
    return str(fut_local_id).encode(), 0


def handle_get_objects(data: bytes, _: int, mock=False) -> tuple[bytes, int]:
    fut_local_id = int(data.decode())
    if fut_local_id not in _futures:
        return b"object_ref not found!", 1
    
    if mock:
        return _futures[fut_local_id]
    else:
        obj_ref = _futures[fut_local_id]  # todo: consider to pop it to avoid memory leak
        logger.debug(f"[Py] get obj {obj_ref.hex()}")
        res, code = ray.get(obj_ref)
        return res, code


def handle(handlers, cmd: int, data: bytes) -> tuple[bytes, int]:
    cmd, index = cmd & cmdBitsMask, cmd >> cmdBitsLen
    logger.debug(f"[py] handle {Go2PyCmd(cmd).name}, {index=}, {len(data)=}, {threading.current_thread().name}")
    func = handlers[cmd]
    try:
        return func(data, index)
    except Exception as e:
        logging.exception(f"[py] handle {Go2PyCmd(cmd).name} error {e}")
        return b'', 0


handlers = {
    Go2PyCmd.CMD_INIT: handle_init,
    Go2PyCmd.CMD_EXECUTE_REMOTE_TASK: functools.partial(handle_run_remote_task, mock=False),
    Go2PyCmd.CMD_GET_OBJECTS: functools.partial(handle_get_objects, mock=False),
    Go2PyCmd.CMD_EXECUTE_PYTHON_CODE: handle_run_python_code,
}

mock_handlers = {
    Go2PyCmd.CMD_INIT: handle_init,
    Go2PyCmd.CMD_EXECUTE_REMOTE_TASK: functools.partial(handle_run_remote_task, mock=True),
    Go2PyCmd.CMD_GET_OBJECTS: functools.partial(handle_get_objects, mock=True),
    Go2PyCmd.CMD_EXECUTE_PYTHON_CODE: handle_run_python_code,
}

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
    elif args.mode == 'local':
        ray.init(runtime_env=ray_runtime_env)

    handlers_ = handlers
    if args.mode == 'mock':
        handlers_ = mock_handlers
    ffi.register_handler(functools.partial(handle, handlers_))

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

