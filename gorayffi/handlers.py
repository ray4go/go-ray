import io
import json
import logging
import struct
import traceback
import typing

import msgpack

from . import funccall, utils, registry
from .consts import *


def run_task(
    func_name: str,
    raw_args: bytes,
) -> tuple[bytes, int]:
    func = registry.get_export_python_func(func_name)
    if func is None:
        return utils.error_msg(f"python task {func_name} not found"), ErrCode.Failed

    args = list(msgpack.Unpacker(io.BytesIO(raw_args), strict_map_key=False))

    try:
        res = func(*args)
    except Exception as e:
        logging.exception(f"[py] execute error {e}")
        return (
            utils.error_msg(f"run python task error: {e}"),
            ErrCode.Failed,
        )

    return msgpack.packb(res, use_bin_type=True), ErrCode.Success


def handle_run_py_local_task(data: bytes) -> tuple[bytes, int]:
    args_data, options = funccall.decode_funccall_arguments(data)
    func_name = options.pop(TASK_NAME_OPTION_KEY)

    return run_task(func_name, args_data)


def handle_run_python_func_code(data: bytes) -> tuple[bytes, int]:
    args_data, options = funccall.decode_funccall_arguments(data)
    func_code = options.pop("func_code")
    func_names = utils.parse_function_name(func_code)
    if len(func_names) == 0:
        return b"Invalid python function code: can't get function name", ErrCode.Failed
    if len(func_names) > 1:
        return b"Invalid python function code: multiple function definitions found", ErrCode.Failed
    func_name = func_names[0]
    args = list(msgpack.Unpacker(io.BytesIO(args_data), strict_map_key=False))
    args_list = ",".join(f"arg{i}" for i in range(len(args)))
    func_call_code = f"__res__ = {func_name}({args_list})"
    args = {f"arg{i}": arg for i, arg in enumerate(args)}
    code = f"{func_code}\n{func_call_code}"
    try:
        """
        Remember that at module level, globals and locals are the same dictionary.
        If exec gets two separate objects as globals and locals,
        the code will be executed as if it were embedded in a class definition.
        https://docs.python.org/3/library/functions.html#exec
        """
        exec(code, args)
    except Exception as e:
        error_string = f"Call python function error: {e}\n" + traceback.format_exc()
        return error_string.encode("utf-8"), ErrCode.Failed

    res = args["__res__"]
    return msgpack.packb(res, use_bin_type=True), ErrCode.Success


handlers = {
    Go2PyCmd.CMD_EXECUTE_PY_LOCAL_TASK: handle_run_py_local_task,
    Go2PyCmd.CMD_EXECUTE_PYTHON_CODE: handle_run_python_func_code,
}


def cmds_dispatcher(
    cmd2handler: dict[int, typing.Callable[[bytes], tuple[bytes, int]]]
) -> typing.Callable[[int, bytes], tuple[bytes, int]]:
    def handler(cmd: int, data: bytes) -> tuple[bytes, int]:
        if cmd not in cmd2handler:
            return utils.error_msg(f"Unknown Go2PyCmd {cmd}"), ErrCode.Failed

        func = cmd2handler[cmd]
        try:
            return func(data)
        except Exception as e:
            error_string = (
                f"[python] handle {Go2PyCmd(cmd).name} error {e}\n" + traceback.format_exc()
            )
            return error_string.encode("utf8"), ErrCode.Failed

    return handler
