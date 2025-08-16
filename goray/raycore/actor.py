import json
import logging

import ray

from .. import funccall, state, utils
from ..consts import *
from . import common

logger = logging.getLogger(__name__)


class _Actor:
    go_instance_index: int
    go_class_idx: int

    def __init__(
        self,
        actor_class_idx: int,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ):
        data, err = funccall.pack_golang_funccall_data(
            raw_args, object_positions, *object_refs
        )
        if err != 0:
            raise Exception(data.decode("utf-8"))

        res, code = common.load_go_lib().execute(
            Py2GoCmd.CMD_NEW_ACTOR, actor_class_idx, data
        )
        logger.debug(f"[py] CMD_NEW_ACTOR {actor_class_idx=}, {res=} {code=}")
        if code != ErrCode.Success:
            raise Exception("go ffi.execute failed: " + res.decode("utf-8"))

        self.go_instance_index = int(res.decode("utf-8"))
        self.go_class_idx = actor_class_idx

    # todo: rename method name to actual name
    def method(
        self,
        method_idx: int,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ) -> tuple[bytes, int]:
        data, err = funccall.pack_golang_funccall_data(
            raw_args, object_positions, *object_refs
        )
        if err != 0:
            return data, err

        request = method_idx | self.go_instance_index << 22
        try:
            res, code = common.load_go_lib().execute(
                Py2GoCmd.CMD_ACTOR_METHOD_CALL, request, data
            )
            logger.debug(
                f"[py] run actor method {method_idx=}, {self.go_instance_index =} {code=}"
            )
        except Exception as e:
            logging.exception(f"[py] execute actor method error {e}")
            return (
                f"[goray error] python ffi.execute() error: {e}".encode("utf-8"),
                ErrCode.Failed,
            )
        return res, code


class Actor:
    _actor: _Actor

    def __init__(self, *args, **kwargs):
        self._actor = _Actor(*args, **kwargs)

    def method(self, *args, **kwargs):
        return self._actor.method(*args, **kwargs)


def handle_new_actor(
    data: bytes, actor_class_idx: int, mock=False
) -> tuple[bytes, int]:
    raw_args, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    logger.debug(f"[py] new actor {actor_class_idx}, {options=}, {object_positions=}")
    common.inject_runtime_env(options)
    actor_type_name = options.pop("goray_actor_type_name", None)
    if mock:
        actor_handle = _Actor(actor_class_idx, raw_args, object_positions, *object_refs)
    else:
        ActorCls = ray.remote(
            common.copy_class(Actor, actor_type_name or "Actor", "Go")
        )
        actor_handle = ActorCls.options(**options).remote(
            actor_class_idx, raw_args, object_positions, *object_refs
        )
    actor_local_id = state.actors.add(actor_handle)
    return str(actor_local_id).encode(), 0


def handle_actor_method_call(
    data: bytes,
    request: int,  # methodIndex: 22bits, PyActorInstanceId:32:bits
    mock=False,
) -> tuple[bytes, int]:
    method_id, actor_local_id = request & ((1 << 22) - 1), request >> 22
    raw_args, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    if "__py_actor_method_name__" in options:
        method_id = options.pop("__py_actor_method_name__")

    logger.debug(
        f"[py] actor method call {actor_local_id=}, {method_id=}, {options=}, {object_positions=}"
    )

    if actor_local_id not in state.actors:
        return utils.error_msg("actor not found!"), ErrCode.Failed

    actor_handle = state.actors[actor_local_id]
    if mock:
        fut = actor_handle.method(method_id, raw_args, object_positions, *object_refs)
    else:
        fut = actor_handle.method.options(**options).remote(
            method_id, raw_args, object_positions, *object_refs
        )
    fut_local_id = state.futures.add(fut)
    return str(fut_local_id).encode(), 0


def handle_kill_actor(
    data: bytes, actor_local_id: int, mock=False
) -> tuple[bytes, int]:
    if actor_local_id not in state.actors:
        return b"actor not found!", ErrCode.Failed
    actor_handle = state.actors[actor_local_id]
    options = json.loads(data)
    if mock:
        return b"", 0

    ray.kill(actor_handle, **options)
    return b"", 0


def _actor_class_name(actor_handle: ray.actor.ActorHandle) -> str:
    try:
        return actor_handle._ray_actor_creation_function_descriptor.class_name
    except Exception as e:
        # Actor(Go.Hello, b6018165d88f27ae4fde1bc801000000)
        return (
            str(actor_handle).removeprefix("Actor(").removesuffix(")").split(",", 1)[0]
        )


def handle_get_actor(data: bytes, _: int, mock=False) -> tuple[bytes, int]:
    options = json.loads(data)
    actor_handle = ray.get_actor(**options)
    actor_local_id = state.actors.add(actor_handle)

    actor_full_name = _actor_class_name(actor_handle)
    # we use actor class_name to indicate the actor language and underlying type.
    # it's not a good way, but we have no better choice.
    lang, name = actor_full_name.split(".", 1)
    res = json.dumps(
        {
            "py_local_id": actor_local_id,
            "actor_type_name": name,
            "is_golang_actor": lang == "Go",
        }
    )
    return res.encode(), 0
