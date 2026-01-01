import functools
import io
import json
import logging
import struct
import traceback
from typing import Callable, Optional

import msgpack

from . import funccall, utils
from .consts import *


def handle_run_py_local_task(
    data: bytes,
    python_func_getter: Callable[[str], Optional[Callable]],
) -> tuple[bytes, int]:
    raw_args, options = funccall.decode_funccall_arguments(data)
    func_name = options.pop(TASK_NAME_OPTION_KEY)

    func = python_func_getter(func_name)
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


def handle_run_python_func_code(data: bytes) -> tuple[bytes, int]:
    args_data, options = funccall.decode_funccall_arguments(data)
    func_code = options.pop("func_code")
    func_names = utils.parse_function_name(func_code)
    if len(func_names) == 0:
        return b"Invalid python function code: can't get function name", ErrCode.Failed
    if len(func_names) > 1:
        return (
            b"Invalid python function code: multiple function definitions found",
            ErrCode.Failed,
        )
    func_name = func_names[0]
    args = list(msgpack.Unpacker(io.BytesIO(args_data), strict_map_key=False))
    args_list = ",".join(f"arg{i}" for i in range(len(args)))
    func_call_code = f"__res__ = {func_name}({args_list})"
    context = {f"arg{i}": arg for i, arg in enumerate(args)}
    code = f"{func_code}\n{func_call_code}"
    try:
        """
        Remember that at module level, globals and locals are the same dictionary.
        If exec gets two separate objects as globals and locals,
        the code will be executed as if it were embedded in a class definition.
        https://docs.python.org/3/library/functions.html#exec
        """
        exec(code, context)
    except Exception as e:
        error_string = f"Call python function error: {e}\n" + traceback.format_exc()
        return error_string.encode("utf-8"), ErrCode.Failed

    res = context["__res__"]
    return msgpack.packb(res, use_bin_type=True), ErrCode.Success


_uint64_le_packer = struct.Struct("<Q")


def handle_new_python_class_instance(
    data: bytes,
    python_class_getter: Callable[[str], Optional[Callable]],
    class_instances_store: utils.ThreadSafeLocalStore,
) -> tuple[bytes, int]:
    args_data, options = funccall.decode_funccall_arguments(data)
    class_name = options.pop(ACTOR_NAME_OPTION_KEY)

    cls = python_class_getter(class_name)
    if cls is None:
        return utils.error_msg(f"python class {class_name} not found"), ErrCode.Failed

    args = list(msgpack.Unpacker(io.BytesIO(args_data), strict_map_key=False))
    try:
        instance = cls(*args)
    except Exception as e:
        return (
            utils.error_msg(f"create python class {class_name} error: {e}"),
            ErrCode.Failed,
        )

    instance_id = class_instances_store.add(instance)
    return _uint64_le_packer.pack(instance_id), ErrCode.Success


def handle_class_instance_method_call(
    data: bytes,
    class_instances_store: utils.ThreadSafeLocalStore,
) -> tuple[bytes, int]:
    args_data, options = funccall.decode_funccall_arguments(data)
    method_name = options.pop(TASK_NAME_OPTION_KEY)
    instance_id = options.pop(PY_LOCAL_ACTOR_ID_KEY)
    if instance_id not in class_instances_store:
        return utils.error_msg("python class instance not found!"), ErrCode.Failed
    obj_handle = class_instances_store[instance_id]
    args = list(msgpack.Unpacker(io.BytesIO(args_data), strict_map_key=False))
    try:
        res = getattr(obj_handle, method_name)(*args)
    except Exception as e:
        return (
            utils.error_msg(f"call python class instance method error: {e}"),
            ErrCode.Failed,
        )

    return msgpack.packb(res, use_bin_type=True), ErrCode.Success


def handle_close_python_class_instance(
    data: bytes,
    class_instances_store: utils.ThreadSafeLocalStore,
) -> tuple[bytes, int]:
    options = json.loads(data)
    instance_id = options.pop(PY_LOCAL_ACTOR_ID_KEY)

    if instance_id not in class_instances_store:
        return utils.error_msg("python class instance not found!"), ErrCode.Failed

    class_instances_store.release(instance_id)
    return b"", 0


def get_handlers(
    python_func_getter: Callable[[str], Optional[Callable]],
    python_class_getter: Callable[[str], Optional[Callable]],
) -> dict[int, Callable[[bytes], tuple[bytes, int]]]:
    class_instances_store = utils.ThreadSafeLocalStore()  # instance_id -> instance
    return {
        Go2PyCmd.CMD_EXECUTE_PYTHON_CODE: handle_run_python_func_code,
        Go2PyCmd.CMD_EXECUTE_PY_LOCAL_TASK: functools.partial(
            handle_run_py_local_task, python_func_getter=python_func_getter
        ),
        Go2PyCmd.CMD_NEW_CLASS_INSTANCE: functools.partial(
            handle_new_python_class_instance,
            python_class_getter=python_class_getter,
            class_instances_store=class_instances_store,
        ),
        Go2PyCmd.CMD_LOCAL_METHOD_CALL: functools.partial(
            handle_class_instance_method_call,
            class_instances_store=class_instances_store,
        ),
        Go2PyCmd.CMD_CLOSE_CLASS_INSTANCE: functools.partial(
            handle_close_python_class_instance,
            class_instances_store=class_instances_store,
        ),
    }
