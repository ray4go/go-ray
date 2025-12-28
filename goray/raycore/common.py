import functools
import io
import struct
import typing

import msgpack

from gorayffi import consts, handlers as cross_lang_handlers, funccall
from gorayffi import ffi
from . import handlers
from .. import state
from ..utils import get_module

uint64_le_packer = struct.Struct("<Q")


@functools.lru_cache(maxsize=None)
def load_go_lib():
    if not state.golibpath:
        raise Exception("init() must be called first")

    lib = ffi.load_go_lib(
        state.golibpath, cross_lang_handlers.cmds_dispatcher(handlers.handlers)
    )

    if state.pymodulepath:
        get_module(state.pymodulepath)

    return lib


def inject_runtime_env(options: dict):
    if options.get("num_returns", 1) > 1:
        raise Exception("num_returns is not supported in goray")

    options.setdefault("runtime_env", {})
    options["runtime_env"].setdefault("env_vars", {})
    options["runtime_env"]["env_vars"][consts.GORAY_BIN_PATH_ENV] = state.golibpath
    options["runtime_env"]["env_vars"][
        consts.GORAY_PY_MUDULE_PATH_ENV
    ] = state.pymodulepath


def copy_function(func, name: str, namespace: str = ""):
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    wrapper.__name__ = name
    if namespace:
        namespace = namespace + "."
    wrapper.__qualname__ = namespace + name
    wrapper.__module__ = "goray"
    return wrapper


def copy_class(cls, name: str, namespace: str = "", **members):
    if namespace:
        namespace = namespace + "."

    members.update(
        {"__module__": "goray", "__qualname__": namespace + name, "__name__": name}
    )
    new_cls = type(
        name,
        (cls,),
        members,
    )
    return new_cls


def method_bind(func, *args, **kwargs):
    """
    bind positional arguments and keyword arguments to a class method
    """

    def wrapper(self, *args2, **kwargs2):
        return func(self, *args, *args2, **kwargs, **kwargs2)

    return wrapper


def get_class_methods(cls):
    return [
        func
        for func in dir(cls)
        if callable(getattr(cls, func)) and not func.startswith("__")
    ]


def _get_objects(arg_pos_to_object: dict[int, dict]):
    object_positions = []
    object_refs = []
    release_obj_ids = set()
    for pos, obj in sorted(arg_pos_to_object.items()):
        object_positions.append(int(pos))
        local_id, auto_release = int(obj["id"]), obj["auto_release"]
        if local_id not in state.futures:
            raise RuntimeError(
                "ObjectRef not found! It might be released, see ObjectRef.DisableAutoRelease() doc for more info"
            )
        object_refs.append(state.futures[local_id])
        if auto_release:
            release_obj_ids.add(local_id)

    # todo: release object refs after the remote call
    for local_id in release_obj_ids:
        state.futures.release(local_id)
    return object_positions, object_refs


def decode_remote_func_call_args(data: bytes):
    raw_args, options = funccall.decode_funccall_arguments(data)
    arg_pos_to_object = options.pop("go_ray_arg_pos_to_object", {})
    object_positions, object_refs = _get_objects(arg_pos_to_object)
    return raw_args, options, object_positions, object_refs


def decode_args(
    raw_args: bytes,
    object_positions: list[int],
    object_refs: typing.Sequence[tuple[bytes, int]],
) -> list:
    unpacker = msgpack.Unpacker(io.BytesIO(raw_args), strict_map_key=False)
    args = []
    for unpacked in unpacker:
        args.append(unpacked)

    for idx, (raw_res, code) in zip(object_positions, object_refs):
        if code != 0:  # ray task for this object failed
            origin_err_msg = raw_res.decode("utf-8")
            err_msg = f"ray task for the object in {idx}th argument error[{consts.ErrCode(code).name}]: {origin_err_msg}"
            raise Exception(err_msg)
        args.insert(idx, msgpack.unpackb(raw_res, strict_map_key=False))
    return args
