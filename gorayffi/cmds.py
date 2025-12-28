"""
expose all golang api to python
"""

import functools
import io
import json
import logging
import typing

import msgpack

from . import funccall, utils
from .consts import *

logger = logging.getLogger(__name__)


class GoCommander:
    def __init__(
        self, cmd_execute_func: typing.Callable[[int, int, bytes], tuple[bytes, int]]
    ):
        self.execute = cmd_execute_func

    @functools.cache
    def get_golang_tasks_info(self) -> tuple[list[str], list[str]]:
        data, err = self.execute(Py2GoCmd.CMD_GET_TASK_ACTOR_LIST, 0, b"")
        if err != 0:
            raise Exception(data.decode("utf-8"))
        return json.loads(data)

    @functools.cache
    def get_golang_actor_methods(self, actor_class_name: str) -> list[str]:
        data, err = self.execute(
            Py2GoCmd.CMD_GET_ACTOR_METHODS, 0, actor_class_name.encode()
        )
        if err != 0:
            raise Exception(data.decode("utf-8"))
        return json.loads(data)

    def call_golang_func(self, func_name: str, args: tuple):
        raw_args = b"".join(msgpack.packb(arg, use_bin_type=True) for arg in args)
        res, code = self.raw_call_golang_func(func_name, raw_args, [])
        if code != 0:
            raise Exception(
                f"execute golang task {func_name} error: {res.decode('utf-8')}"
            )
        returns = list(msgpack.Unpacker(io.BytesIO(res), strict_map_key=False))
        if len(returns) == 1:
            return returns[0]
        elif len(returns) == 0:
            return None
        return returns

    def raw_call_golang_func(
        self,
        func_name: str,
        raw_args: bytes,
        object_positions: list[int],
        *resolved_object_refs: tuple[bytes, int],
    ) -> tuple[bytes, int]:
        """
        low level api to call golang func

        - arguments and return values are encoded raw bytes
        - extra arguments to pass resolved ray object data
        """
        data, err = funccall.encode_golang_funccall_arguments(
            func_name, raw_args, object_positions, *resolved_object_refs
        )
        if err != 0:
            return data, err

        logger.debug(f"[py] local_run_task {func_name=}, {object_positions=}")
        try:
            res, code = self.execute(Py2GoCmd.CMD_RUN_TASK, 0, data)
        except Exception as e:
            logging.exception(f"[py] python ffi.execute(CMD_RUN_TASK) error {e}")
            return (
                f"[goray error] python ffi.execute(CMD_RUN_TASK) error: {e}".encode(
                    "utf-8"
                ),
                ErrCode.Failed,
            )
        return res, code

    def new_golang_actor(
        self,
        actor_class_name: str,
        encoded_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ):
        """
        create a new golang actor instance, arguments are encoded raw bytes, return the go obj id
        """
        data, err = funccall.encode_golang_funccall_arguments(
            actor_class_name,
            encoded_args,
            object_positions,
            *object_refs,
        )
        if err != 0:
            raise Exception(data.decode("utf-8"))
        logger.debug(f"[py] new golang actor {actor_class_name}")

        res, code = self.execute(Py2GoCmd.CMD_NEW_ACTOR, 0, data)
        if code != ErrCode.Success:
            raise Exception("create golang actor error: " + res.decode("utf-8"))

        return int(res.decode("utf-8"))

    def raw_call_golang_method(
        self,
        go_obj_id: int,
        method_name: str,
        encoded_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ) -> tuple[bytes, int]:
        """
        low level api to call golang actor method, arguments and return values are encoded raw bytes
        """
        data, err = funccall.encode_golang_funccall_arguments(
            method_name, encoded_args, object_positions, *object_refs
        )
        if err != 0:
            return data, err

        logger.debug(f"[py] run actor method {method_name=}, {go_obj_id=}")
        try:
            res, code = self.execute(Py2GoCmd.CMD_ACTOR_METHOD_CALL, go_obj_id, data)
        except Exception as e:
            logging.exception(
                f"[py] python ffi.execute(CMD_ACTOR_METHOD_CALL) error {e}"
            )
            return (
                utils.error_msg(
                    f"python ffi.execute(CMD_ACTOR_METHOD_CALL) error: {e}"
                ),
                ErrCode.Failed,
            )
        return res, code

    def close_actor(self, go_obj_id: int) -> tuple[str, int]:
        """close the actor instance"""
        res, code = self.execute(Py2GoCmd.CMD_CLOSE_ACTOR, go_obj_id, b"")
        return res.decode("utf8"), code

    def get_init_options(self) -> dict[str, typing.Any]:
        """get init options from go ray.Init()"""
        data, code = self.execute(Py2GoCmd.CMD_GET_INIT_OPTIONS, 0, b"")
        if code != 0:
            raise Exception(f"CMD_GET_INIT_OPTIONS error: {data.decode('utf-8')}")
        return json.loads(data)

    def start_driver(self) -> tuple[str, int]:
        """start the driver function register in go ray.Init()"""
        data, code = self.execute(Py2GoCmd.CMD_START_DRIVER, 0, b"")
        return data.decode("utf8"), code
