import io
import logging
import traceback

import msgpack

from .. import funccall, utils
from ..consts import *

# name to function
_python_export_funcs = {}


def add_python_export_func(func):
    global _python_export_funcs
    _python_export_funcs[func.__name__] = func


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


def decode_args(
    raw_args: bytes, object_positions: list[int], object_refs: list[tuple[bytes, int]]
) -> list:
    reader = io.BytesIO(raw_args)
    unpacker = msgpack.Unpacker(reader)
    args = []
    for unpacked in unpacker:
        args.append(unpacked)

    for idx, (raw_res, code) in zip(object_positions, object_refs):
        if code != 0:  # ray task for this object failed
            origin_err_msg = raw_res.decode("utf-8")
            err_msg = f"ray task for the object in {idx}th argument error[{ErrCode(code).name}]: {origin_err_msg}"
            raise Exception(err_msg)
        args.insert(idx, msgpack.unpackb(raw_res))
    return args


def run_task(
    func_name: str,
    raw_args: bytes,
    object_positions: list[int],
    *object_refs: tuple[bytes, int],
) -> tuple[bytes, int]:
    global _python_export_funcs

    func = _python_export_funcs.get(func_name)
    if func is None:
        return f"[py] task {func_name} not found".encode("utf-8"), ErrCode.Failed

    args = decode_args(raw_args, object_positions, object_refs)

    try:
        res = func(*args)
    except Exception as e:
        logging.exception(f"[py] execute error {e}")
        return (
            f"[goray error] python run task error: {e}".encode("utf-8"),
            ErrCode.Failed,
        )

    return msgpack.packb(res, use_bin_type=True), ErrCode.Success


def handle_run_py_local_task(data: bytes, _: int) -> tuple[bytes, int]:
    args_data, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    func_name = options.pop("task_name")
    return run_task(func_name, args_data, object_positions, *object_refs)


handlers = {
    Go2PyCmd.CMD_EXECUTE_PY_LOCAL_TASK: handle_run_py_local_task,
    Go2PyCmd.CMD_EXECUTE_PYTHON_CODE: handle_run_python_code,
}


def handle(cmd: int, index: int, data: bytes) -> tuple[bytes, int]:
    if cmd not in handlers:
        return (
            utils.error_msg(
                f"Go2PyCmd {Go2PyCmd(cmd).name} is not available in cross lang only mode"
            ),
            ErrCode.Failed,
        )

    func = handlers[cmd]
    try:
        return func(data, index)
    except Exception as e:
        error_string = (
            f"[python] handle {Go2PyCmd(cmd).name} error {e}\n" + traceback.format_exc()
        )
        # logger.error(error_string)
        return error_string.encode("utf8"), ErrCode.Failed
