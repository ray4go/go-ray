import json
import logging

import ray

from . import common
from .. import funccall, state, utils
from ..consts import *

logger = logging.getLogger(__name__)


class Actor:
    go_instance_index: int
    actor_type_name: str

    def __init__(
        self,
        actor_type_name: str,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ):
        data, err = funccall.pack_golang_funccall_data(
            actor_type_name, raw_args, object_positions, *object_refs
        )
        if err != 0:
            raise Exception(data.decode("utf-8"))

        res, code = common.load_go_lib().execute(Py2GoCmd.CMD_NEW_ACTOR, 0, data)
        logger.debug(f"[py] CMD_NEW_ACTOR {actor_type_name}, {res=} {code=}")
        if code != ErrCode.Success:
            raise Exception("go ffi.execute failed: " + res.decode("utf-8"))

        self.go_instance_index = int(res.decode("utf-8"))
        self.actor_type_name = actor_type_name

    # todo: rename method name to actual name
    def method(
        self,
        method_name: str,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ) -> tuple[bytes, int]:
        data, err = funccall.pack_golang_funccall_data(
            method_name, raw_args, object_positions, *object_refs
        )
        if err != 0:
            return data, err

        logger.debug(f"[py] run actor method {method_name}, {self.go_instance_index =}")
        try:
            res, code = common.load_go_lib().execute(
                Py2GoCmd.CMD_ACTOR_METHOD_CALL, self.go_instance_index, data
            )
        except Exception as e:
            logging.exception(f"[py] execute actor method error {e}")
            return (
                utils.error_msg(f"execute actor method error: {e}"),
                ErrCode.Failed,
            )
        return res, code


def handle_new_actor(data: bytes, _: int, mock=False) -> tuple[bytes, int]:
    raw_args, options, object_positions, object_refs = funccall.decode_funccall_args(
        data
    )
    actor_type_name = options.pop(ACTOR_NAME_OPTION_KEY)
    logger.debug(f"[py] new actor {actor_type_name}, {options=}, {object_positions=}")

    common.inject_runtime_env(options)
    ActorCls = ray.remote(common.copy_class(Actor, actor_type_name, Language.GO))
    actor_handle = ActorCls.options(**options).remote(
        actor_type_name, raw_args, object_positions, *object_refs
    )
    actor_local_id = state.actors.add(actor_handle)
    return str(actor_local_id).encode(), 0


def handle_actor_method_call(
    data: bytes,
    _: int,  # methodIndex: 22bits, PyActorInstanceId:32:bits
    mock=False,
) -> tuple[bytes, int]:
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
    fut = actor_handle.method.options(**options).remote(
        method_name, raw_args, object_positions, *object_refs
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
