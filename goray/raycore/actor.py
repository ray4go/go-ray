import json
import logging

import ray

from . import common, actor_wrappers
from .. import funccall, state, utils
from ..consts import *
from ..x import actor

logger = logging.getLogger(__name__)


def handle_new_actor(data: bytes) -> tuple[bytes, int]:
    encoded_args, options, object_positions, object_refs = (
        funccall.decode_funccall_args(data)
    )
    actor_type_name = options.pop(ACTOR_NAME_OPTION_KEY)
    actor_methods = options.pop(ACTOR_METHOD_LIST_OPTION_KEY)
    logger.debug(f"[py] new actor {actor_type_name}, {options=}, {object_positions=}")

    common.inject_runtime_env(options)
    ActorCls = actor_wrappers.new_remote_actor_type(
        actor_wrappers.GoActor,
        actor_type_name,
        actor_methods,
        namespace=ActorSourceLang.GO,
    )
    actor_handle = ActorCls.options(**options).remote(
        actor_type_name,
        actor.CallerLang.Golang,
        encoded_args,
        object_positions,
        *object_refs,
    )
    actor_local_id = state.actors.add(actor_handle)
    return common.uint64_le_packer.pack(actor_local_id), 0


def handle_actor_method_call(data: bytes) -> tuple[bytes, int]:
    raw_args, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    method_name = options.pop(TASK_NAME_OPTION_KEY)
    actor_local_id = options.pop(PY_LOCAL_ACTOR_ID_KEY)
    logger.debug(
        f"[py] actor method call {actor_local_id=}, {method_name}, {options=}, {object_positions=}"
    )
    if actor_local_id not in state.actors:
        return utils.error_msg("actor not found!"), ErrCode.Failed

    actor_handle = state.actors[actor_local_id]
    method = getattr(actor_handle, method_name + actor_wrappers.METHOD_WITH_ENCODED_ARGS_SUFFIX)
    fut = method.options(**options).remote(
        method_name, raw_args, object_positions, *object_refs
    )
    fut_local_id = state.futures.add(fut)
    return common.uint64_le_packer.pack(fut_local_id), 0


def handle_kill_actor(data: bytes) -> tuple[bytes, int]:
    options = json.loads(data)
    actor_local_id = options.pop(PY_LOCAL_ACTOR_ID_KEY)
    if actor_local_id not in state.actors:
        return utils.error_msg(f"actor id {actor_local_id} not found!"), ErrCode.Failed
    actor_handle = state.actors[actor_local_id]
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


def handle_get_actor(data: bytes) -> tuple[bytes, int]:
    options = json.loads(data)
    actor_handle = ray.get_actor(**options)
    actor_local_id = state.actors.add(actor_handle)

    actor_full_name = _actor_class_name(actor_handle)
    # we use actor class_name to indicate the actor language and underlying type.
    # it's not a good way, but we have no better choice.
    try:
        source, name = actor_full_name.split(".", 1)
        assert source in (ActorSourceLang.GO, ActorSourceLang.PY)
    except Exception:
        return (
            utils.error_msg(
                f"Invalid actor {actor_full_name!r}, this actor is not created by goray."
            ),
            ErrCode.Failed,
        )

    res = json.dumps(
        {
            "py_local_id": actor_local_id,
            "actor_type_name": name,
            "is_golang_actor": source == ActorSourceLang.GO,
        }
    )
    return res.encode(), 0
