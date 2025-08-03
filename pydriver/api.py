import functools
import io
import json
import logging
import inspect

import msgpack
import ray

from . import funccall
from . import state
from . import utils
from .consts import *

logger = logging.getLogger(__name__)
utils.init_logger(logger)

# name -> (func_or_class, options)
_user_tasks_actors = {}


def _make_remote(function_or_class, options: dict):
    _user_tasks_actors[function_or_class.__name__] = (function_or_class, options)
    if options:
        return ray.remote(**options)(function_or_class)
    else:
        return ray.remote(function_or_class)


def remote(*args, **kwargs):
    """
    Same as @ray.remote, but with registering the task or actor in goray, which you can call from go.
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @remote.
        # "args[0]" is the class or function under the decorator.
        return _make_remote(args[0], {})
    return functools.partial(_make_remote, options=kwargs)


@ray.remote
def ray_run_task_from_go(
    func_name: str,
    raw_args: bytes,
    object_positions: list[int],
    *object_refs: list[tuple[bytes, int]],
) -> tuple[bytes, int]:
    from . import main

    main.init_ffi_once()

    return run_task(func_name, raw_args, object_positions, *object_refs)


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
    global _user_tasks_actors

    print(f"run_task {func_name}, all task {_user_tasks_actors=}")
    func, _ = _user_tasks_actors.get(func_name)
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


def handle_run_py_task(data: bytes, _: int, mock=False) -> tuple[bytes, int]:
    args_data, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    func_name = options.pop("task_name")
    _, opts = _user_tasks_actors.get(func_name)
    opts = dict(opts)
    opts.update(options)
    if mock:
        fut = run_task(func_name, args_data, object_positions, *object_refs)
    else:
        fut = ray_run_task_from_go.options(**opts).remote(
            func_name, args_data, object_positions, *object_refs
        )
    fut_local_id = state.futures.add(fut)
    return str(fut_local_id).encode(), 0


def handle_run_py_local_task(data: bytes, _: int, mock=False) -> tuple[bytes, int]:
    args_data, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    func_name = options.pop("task_name")
    return run_task(func_name, args_data, object_positions, *object_refs)


@ray.remote
class PyActorWrapper:

    def __init__(
        self,
        class_name: str,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ):
        from . import main

        main.init_ffi_once()

        if class_name not in _user_tasks_actors:
            raise Exception(f"python actor {class_name} not found")
        cls, opts = _user_tasks_actors[class_name]
        if not inspect.isclass(cls):
            raise Exception(f"python actor {class_name} not found")

        args = decode_args(raw_args, object_positions, object_refs)
        self._actor = cls(*args)

    def method(
        self,
        method_name: str,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ) -> tuple[bytes, int]:
        args = decode_args(raw_args, object_positions, object_refs)
        try:
            res = getattr(self._actor, method_name)(*args)
        except Exception as e:
            logging.exception(f"[py] execute error {e}")
            return (
                f"[goray error] python run task error: {e}".encode("utf-8"),
                ErrCode.Failed,
            )
        return msgpack.packb(res, use_bin_type=True), ErrCode.Success

    def get_go_class_index(self):
        return -1


def handle_new_py_actor(data: bytes, _: int, mock=False) -> tuple[bytes, int]:
    args_data, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    class_name = options.pop("actor_class_name")
    if class_name not in _user_tasks_actors:
        return f"python actor {class_name} not found".encode("utf-8"), ErrCode.Failed
    cls, opts = _user_tasks_actors[class_name]
    if not inspect.isclass(cls):
        return f"python actor {class_name} not found".encode("utf-8"), ErrCode.Failed

    actor_handle = PyActorWrapper.options(**options).remote(
        class_name, args_data, object_positions, *object_refs
    )
    actor_local_id = state.actors.add(actor_handle)
    return str(actor_local_id).encode(), 0


@functools.cache
def get_golang_tasks_info() -> tuple[dict[str, int], dict[str, int]]:
    from . import ffi

    data, err = ffi.execute(Py2GoCmd.CMD_GET_TASK_ACTOR_LIST, b"")
    if err != 0:
        raise Exception(data.decode("utf-8"))
    return json.loads(data)


@functools.cache
def get_golang_actor_methods(actor_class_idx: int) -> dict[str, int]:
    from . import ffi

    cmdBitsLen = 10  # todo
    request = Py2GoCmd.CMD_GET_ACTOR_METHODS | actor_class_idx << cmdBitsLen
    data, err = ffi.execute(request, b"")
    if err != 0:
        raise Exception(data.decode("utf-8"))
    return json.loads(data)


def _golang_local_task(func_id: int, args: tuple):
    from . import ffi

    raw_args = b"".join(msgpack.packb(arg, use_bin_type=True) for arg in args)
    data, code = funccall.pack_golang_funccall_data(raw_args, [], {})
    assert code == 0

    cmdBitsLen = 10  # todo
    cmd = Py2GoCmd.CMD_RUN_TASK | func_id << cmdBitsLen
    res, code = ffi.execute(cmd, data)
    if code != 0:
        raise Exception(f"golang task {func_id} error: {res.decode('utf-8')}")
    returns = list(msgpack.Unpacker(io.BytesIO(res)))
    if len(returns) == 1:
        return returns[0]
    return returns


def golang_local_task(name: str, *args):
    tasks_name2idx, actors_name2idx = get_golang_tasks_info()
    func_id = tasks_name2idx[name]
    return _golang_local_task(func_id, args)


@ray.remote
def _golang_remote_task(func_id: int, *args):
    from . import main

    main.init_ffi_once()
    return _golang_local_task(func_id, args)


def golang_task(name: str) -> 'GolangRemoteFunc':
    tasks_name2idx, actors_name2idx = get_golang_tasks_info()
    func_id = tasks_name2idx[name]
    return GolangRemoteFunc(_golang_remote_task, func_id)


class GolangRemoteFunc:
    def __init__(self, remote_callable_handle, bind_arg, **options):
        self._remote_handle = remote_callable_handle
        self._bind_arg = bind_arg
        self._options = options

    def options(self, **kwargs) -> "GolangRemoteFunc":
        options = dict(self._options)
        options.update(kwargs)
        return GolangRemoteFunc(self._remote_handle, self._bind_arg, **options)

    def remote(self, *args):
        return self._remote_handle.options(**self._options).remote(
            self._bind_arg, *args
        )


@ray.remote
class _RemoteActor:
    def __init__(self, actor_class_name: str, *args):
        self._actor = GolangLocalActor(actor_class_name, *args)

    def call_method(self, method_name: str, *args):
        return self._actor._call_method(method_name, *args)


class GolangLocalActor:
    def __init__(self, actor_class_name: str, *args):
        from . import main

        main.init_ffi_once()

        tasks_name2idx, actors_name2idx = get_golang_tasks_info()
        actor_class_idx = actors_name2idx[actor_class_name]
        self._actor_class_name = actor_class_name
        self._method_name2index = get_golang_actor_methods(actor_class_idx)
        raw_args = b"".join(msgpack.packb(arg, use_bin_type=True) for arg in args)
        self._actor = main._Actor(
            actor_class_idx,
            raw_args=raw_args,
            object_positions=[],
        )

    def _call_method(self, method_name: str, *args):
        method_idx = self._method_name2index[method_name]
        raw_args = b"".join(msgpack.packb(arg, use_bin_type=True) for arg in args)
        res, code = self._actor.method(
            method_idx,
            raw_args=raw_args,
            object_positions=[],
        )
        if code != ErrCode.Success:
            raise Exception(
                f"golang actor method {method_name} error: {res.decode('utf-8')}"
            )
        returns = list(msgpack.Unpacker(io.BytesIO(res)))
        if len(returns) == 1:
            return returns[0]
        return returns

    def __getattr__(self, name):
        if name not in self._method_name2index:
            raise AttributeError(
                f"golang actor type {self._actor_class_name!r} has no method {name!r}"
            )
        return functools.partial(self._call_method, name)

    def __repr__(self):
        return f"<GolangLocalActor {self._actor_class_name} id={self._actor.go_instance_index}>"

    def __del__(self):
        from . import ffi

        request = Py2GoCmd.CMD_CLOSE_ACTOR | self._actor.go_instance_index << 10
        res, code = ffi.execute(request, b"")
        if code != 0:
            logging.error(
                f"close actor {self._actor_class_name} error: {res.decode('utf-8')}"
            )


class GolangRemoteActorHandle:
    def __init__(self, actor_handle: _RemoteActor):
        self._actor = actor_handle

    def __getattr__(self, method_name: str) -> "GolangRemoteFunc":
        return GolangRemoteFunc(self._actor.call_method, method_name)


class GolangActorClass:
    def __init__(self, class_name: str, **options):
        self._class_name = class_name
        self._options = options

    def options(self, **kwargs) -> "GolangActorClass":
        options = dict(self._options)
        options.update(kwargs)
        return GolangActorClass(self._class_name, **options)

    def remote(self, *args, **kwargs) -> "GolangRemoteActorHandle":
        from . import main

        main.init_ffi_once()

        tasks_name2idx, actors_name2idx = get_golang_tasks_info()
        if self._class_name not in actors_name2idx:
            raise Exception(f"golang actor {self._class_name} not found")

        actor_handle = _RemoteActor.options(**self._options).remote(
            self._class_name, *args
        )
        return GolangRemoteActorHandle(actor_handle)


def golang_actor_class(name: str, **options) -> GolangActorClass:
    return GolangActorClass(name, **options)
