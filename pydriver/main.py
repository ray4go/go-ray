import argparse
import functools
import io
import json
import logging
import os
import threading
import time
import traceback

import ray

from . import consts
from . import libpath
from . import utils
from .consts import *

cmdBitsLen = 10
cmdBitsMask = (1 << cmdBitsLen) - 1

logger = logging.getLogger(__name__)
utils.init_logger(logger)


@ray.remote
def show(data):
    # debug
    print("=" * 10, data)


def handle_init(data: bytes, _: int) -> tuple[bytes, int]:
    logger.debug(f"[py] init")
    return b"", 0


def handle_run_python_code(code: bytes, _: int) -> tuple[bytes, int]:
    stream = io.StringIO()
    injects = {
        "write": lambda x: stream.write(str(x)),
    }
    try:
        """
        Remember that at module level, globals and locals are the same dictionary.
        If exec gets two separate objects as globals and locals,
        the code will be executed as if it were embedded in a class definition.
        https://docs.python.org/3/library/functions.html#exec
        """
        exec(code.decode("utf-8"), injects)
    except Exception as e:
        return f"[goray error] python exec() error: {e}".encode("utf-8"), 1

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
def ray_run_task(
    func_id: int,
    raw_args: bytes,
    object_positions: list[int],
    *object_refs: list[tuple[bytes, int]],
) -> tuple[bytes, int]:
    """
    :param func_id:
    :param data:
    :param object_positions: 调用的go task func中, 传入的object_ref作为参数的位置列表, 有序
    :param object_refs: object_ref列表, 会被ray core替换为实际的数据
    :return:
    """
    init_ffi_once()
    return run_task(func_id, raw_args, object_positions, *object_refs)


def run_task(
    func_id: int,
    raw_args: bytes,
    object_positions: list[int],
    *object_refs: list[tuple[bytes, int]],
) -> tuple[bytes, int]:
    data, err = pack_golang_funccall_data(
        raw_args, object_positions, *object_refs
    )
    if err != 0:
        return data, err

    from . import ffi

    cmd = Py2GoCmd.CMD_RUN_TASK | func_id << cmdBitsLen
    try:
        res, code = ffi.execute(cmd, data)
        logger.debug(f"[py] local_run_task {func_id=}, {res=} {code=}")
    except Exception as e:
        logging.exception(f"[py] execute error {e}")
        return f"[goray error] python ffi.execute() error: {e}".encode("utf-8"), ErrCode.Failed
    return res, code


def pack_golang_funccall_data(
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: list[tuple[bytes, int]],
) -> tuple[bytes, int]:
    """
    data format: multiple bytes units
    - first unit is raw args data;
    - other units are objectRefs resolved data;
        - resolved data format: | arg_pos:8byte:int64 | data:[]byte |
    """
    data = [raw_args]
    for pos, (raw_res, code) in zip(object_positions, object_refs):
        if code != 0:  # ray task for this object failed
            origin_err_msg = raw_res.decode("utf-8")
            err_msg = (
                f"ray task for the object in {pos}th argument error: {origin_err_msg}"
            )
            return err_msg.encode("utf-8"), code
        data.append(pos.to_bytes(8, byteorder="little") + raw_res)
    return utils.pack_bytes_units(data), 0




class _Actor:
    go_instance_index:int

    def __init__(
            self,
            actor_class_idx: int,
            raw_args: bytes,
            object_positions: list[int],
            *object_refs: list[tuple[bytes, int]],
    ):
        data, err = pack_golang_funccall_data(
            raw_args, object_positions, *object_refs
        )
        if err != 0:
            raise Exception(data.decode("utf-8"))
        # init_ffi_once()

        from . import ffi
        cmd = Py2GoCmd.CMD_NEW_ACTOR | actor_class_idx << cmdBitsLen
        res, code = ffi.execute(cmd, data)
        logger.debug(f"[py] CMD_NEW_ACTOR {actor_class_idx=}, {res=} {code=}")
        if code != ErrCode.Success:
            raise Exception("go ffi.execute failed: "+res.decode("utf-8"))
        self.go_instance_index = int(res.decode("utf-8"))

    def method(
            self,
            method_idx: int,
            raw_args: bytes,
            object_positions: list[int],
            *object_refs: list[tuple[bytes, int]],
    ) -> tuple[bytes, int]:
        data, err = pack_golang_funccall_data(
            raw_args, object_positions, *object_refs
        )
        if err != 0:
            return data, err

        from . import ffi

        cmd = Py2GoCmd.CMD_ACTOR_METHOD_CALL | method_idx << cmdBitsLen | self.go_instance_index << 32
        try:
            res, code = ffi.execute(cmd, data)
            logger.debug(f"[py] run actor method {method_idx=}, {self.go_instance_index =} {code=}")
        except Exception as e:
            logging.exception(f"[py] execute actor method error {e}")
            return f"[goray error] python ffi.execute() error: {e}".encode("utf-8"), ErrCode.Failed
        return res, code

@ray.remote
class Actor:
    _actor: _Actor
    def __init__(self, *args, **kwargs):
        init_ffi_once()
        self._actor = _Actor(*args, **kwargs)
    def method(self, *args, **kwargs):
        return self._actor.method(*args, **kwargs)

_actors = utils.ThreadSafeLocalStore()

def handle_new_actor(data: bytes, actor_class_idx: int, mock=False) -> tuple[bytes, int]:
    raw_args, options, object_positions, object_refs = decode_funccall_args(data)
    logger.debug(f"[py] new actor {actor_class_idx}, {options=}, {object_positions=}")

    if mock:
        actor_handle = _Actor(
            actor_class_idx, raw_args, object_positions, *object_refs
        )
    else:
        actor_handle = Actor.options(**options).remote(
            actor_class_idx, raw_args, object_positions, *object_refs
        )
    actor_local_id = _actors.add(actor_handle)
    return str(actor_local_id).encode(), 0

def handle_actor_method_call(
    data: bytes,
    request: int,  # methodIndex: 22bits, PyActorId:32:bits
    mock=False,
) -> tuple[bytes, int]:
    method_idx, actor_local_id = request & ((1 << 22) - 1), request >> 22
    raw_args, options, object_positions, object_refs = decode_funccall_args(data)
    logger.debug(f"[py] actor method call {actor_local_id=}, {method_idx=}, {options=}, {object_positions=}")

    if actor_local_id not in _actors:
        return b"actor not found!", ErrCode.Failed

    actor_handle = _actors[actor_local_id]
    if mock:
        fut = actor_handle.method(method_idx, raw_args, object_positions, *object_refs)
    else:
        fut = actor_handle.method.options(**options).remote(
            method_idx, raw_args, object_positions, *object_refs
        )
    fut_local_id = _futures.add(fut)
    return str(fut_local_id).encode(), 0


# in mock mode, value is actual return value, i.e. (data, code).
# in other modes, value is ray object ref
_futures = utils.ThreadSafeLocalStore()

def _get_objects(object_pos_to_local_id: dict[int, int]):
    object_positions = []
    object_refs = []
    for pos, local_id in sorted(object_pos_to_local_id.items()):
        object_positions.append(int(pos))
        if local_id not in _futures:
            return b"object_ref not found!", 1
        object_refs.append(_futures[local_id])
    return object_positions, object_refs


def decode_funccall_args(data: bytes):
    raw_args, opts_data = utils.unpack_bytes_units(data)
    options = json.loads(opts_data)
    object_pos_to_local_id = options.pop("go_ray_object_pos_to_local_id", {})
    object_positions, object_refs = _get_objects(object_pos_to_local_id)
    return raw_args, options, object_positions, object_refs

def handle_run_remote_task(data: bytes, func_id: int, mock=False) -> tuple[bytes, int]:
    args_data, options, object_positions, object_refs = decode_funccall_args(data)
    logger.debug(f"[py] run remote task {func_id}, {options=}, {object_positions=}")
    if mock:
        fut = run_task(func_id, args_data, object_positions, *object_refs)
    else:
        fut = ray_run_task.options(**options).remote(
            func_id, args_data, object_positions, *object_refs
        )
    fut_local_id = _futures.add(fut)
    # show.remote(fut)
    return str(fut_local_id).encode(), 0


def handle_get_objects(data: bytes, _: int, mock=False) -> tuple[bytes, int]:
    fut_local_id, timeout = json.loads(data)
    if fut_local_id not in _futures:
        return b"object_ref not found!", 1

    if mock:
        return _futures[fut_local_id]
    else:
        obj_ref = _futures[
            fut_local_id
        ]  # todo: consider to pop it to avoid memory leak
        logger.debug(f"[Py] get obj {obj_ref.hex()}")
        if timeout == -1:
            timeout = None
        try:
            res, code = ray.get(obj_ref, timeout=timeout)
        except ray.exceptions.GetTimeoutError:
            return b"timeout to get object", consts.ErrCode.Timeout
        except ray.exceptions.TaskCancelledError:
            return b"task cancelled", consts.ErrCode.Cancelled
        return res, code


def handle_put_object(data: bytes, _: int, mock=False) -> tuple[bytes, int]:
    if mock:
        fut = data, 0
    else:
        fut = ray.put([data, 0])
    # side effect: make future outlive this function (on purpose)
    fut_local_id = _futures.add(fut)
    return str(fut_local_id).encode(), 0


def handle_wait_object(data: bytes, _: int, mock=False) -> tuple[bytes, int]:
    opts = json.loads(data)
    fut_local_ids = opts.pop("object_ref_local_ids")

    if mock:
        return json.dumps([fut_local_ids, []]).encode(), 0

    futs = []
    fut_hex2idx = {}
    for idx, fut_local_id in enumerate(fut_local_ids):
        if fut_local_id not in _futures:
            return b"object_ref not found!", 1
        fut = _futures[fut_local_id]
        futs.append(fut)
        fut_hex2idx[fut.hex()] = idx

    ready, not_ready = ray.wait(futs, **opts)

    ready_ids = [fut_hex2idx[i.hex()] for i in ready]
    not_ready_ids = [fut_hex2idx[i.hex()] for i in not_ready]
    ret_data = json.dumps([ready_ids, not_ready_ids]).encode()
    return ret_data, 0


def handle_cancel_object(data: bytes, _: int, mock=False) -> tuple[bytes, int]:
    opts = json.loads(data)
    fut_local_id = opts.pop("object_ref_local_id")
    if fut_local_id not in _futures:
        return b"object_ref not found!", 1
    fut = _futures[fut_local_id]
    if not mock:
        ray.cancel(fut, **opts)
    return b"", 0


def handle(handlers, cmd: int, data: bytes) -> tuple[bytes, int]:
    cmd, index = cmd & cmdBitsMask, cmd >> cmdBitsLen
    logger.debug(
        f"[py] handle {Go2PyCmd(cmd).name}, {index=}, {len(data)=}, {threading.current_thread().name}"
    )
    func = handlers[cmd]
    try:
        return func(data, index)
    except Exception as e:
        error_string = (
            f"[python] handle {Go2PyCmd(cmd).name} error {e}\n" + traceback.format_exc()
        )
        logger.error(error_string)
        return error_string.encode("utf8"), ErrCode.Failed


handlers = {
    Go2PyCmd.CMD_INIT: handle_init,
    Go2PyCmd.CMD_EXECUTE_REMOTE_TASK: functools.partial(
        handle_run_remote_task, mock=False
    ),
    Go2PyCmd.CMD_GET_OBJECT: functools.partial(handle_get_objects, mock=False),
    Go2PyCmd.CMD_PUT_OBJECT: functools.partial(handle_put_object, mock=False),
    Go2PyCmd.CMD_WAIT_OBJECT: functools.partial(handle_wait_object, mock=False),
    Go2PyCmd.CMD_CANCEL_OBJECT: functools.partial(handle_cancel_object, mock=False),

    Go2PyCmd.CMD_NEW_ACTOR: handle_new_actor,
    Go2PyCmd.CMD_ACTOR_METHOD_CALL: handle_actor_method_call,

    Go2PyCmd.CMD_EXECUTE_PYTHON_CODE: handle_run_python_code,
}

mock_handlers = {
    Go2PyCmd.CMD_INIT: handle_init,
    Go2PyCmd.CMD_EXECUTE_REMOTE_TASK: functools.partial(
        handle_run_remote_task, mock=True
    ),
    Go2PyCmd.CMD_GET_OBJECT: functools.partial(handle_get_objects, mock=True),
    Go2PyCmd.CMD_PUT_OBJECT: functools.partial(handle_put_object, mock=True),
    Go2PyCmd.CMD_WAIT_OBJECT: functools.partial(handle_wait_object, mock=True),
    Go2PyCmd.CMD_CANCEL_OBJECT: functools.partial(handle_cancel_object, mock=True),

    Go2PyCmd.CMD_NEW_ACTOR:functools.partial(handle_new_actor, mock=True),
    Go2PyCmd.CMD_ACTOR_METHOD_CALL: functools.partial(handle_actor_method_call, mock=True),

    Go2PyCmd.CMD_EXECUTE_PYTHON_CODE: handle_run_python_code,
}


def main():
    parser = argparse.ArgumentParser(
        description="Python driver for ray-core-go application.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["cluster", "local", "mock", "debug"],
        default="cluster",
        help="指定运行模式：\n"
        "  cluster: 在集群模式下运行\n"
        "  local: 在ray本地模式下运行\n"
        "  mock: 在模拟模式下运行",
    )
    parser.add_argument(
        "go_binary_path",  # 位置参数
        type=str,
        help="指定 Goray 应用二进制文件的路径 (使用 go build -buildmode=c-shared 构建)",
    )
    args = parser.parse_args()

    libpath.golibpath = args.go_binary_path
    from . import ffi

    ray_runtime_env = {"env_vars": {"GORAY_BIN_PATH": args.go_binary_path}}
    if args.debug:
        ray_runtime_env["env_vars"]["GORAY_DEBUG_LOGGING"] = "1"
        utils.enable_logger(logger)
        utils.enable_logger(ffi.logger)

    if args.mode == "cluster":
        ray.init(address="auto", runtime_env=ray_runtime_env)
    elif args.mode == "local":
        ray.init(runtime_env=ray_runtime_env)

    handlers_ = handlers
    if args.mode in ("mock", "debug"):
        handlers_ = mock_handlers
    ffi.register_handler(functools.partial(handle, handlers_))

    if args.mode == "debug":
        print(f"pid: {os.getpid()}")
        time.sleep(2)  # wait for dlv to attach

    try:
        data, code = ffi.execute(Py2GoCmd.CMD_START_DRIVER, b"")
        if code != 0:
            err_msg = data.decode("utf-8")
            logger.debug(f"[py] driver error[{code}]: {err_msg}")
            exit(1)
    except KeyboardInterrupt:
        logger.debug("Exiting...")


if __name__ == "__main__":
    main()
