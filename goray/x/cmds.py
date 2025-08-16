import functools
import io
import json
import logging
import typing

import msgpack

from .. import funccall
from ..consts import *

logger = logging.getLogger(__name__)


class GoCommander:
    def __init__(
        self, cmd_execute_func: typing.Callable[[int, int, bytes], tuple[bytes, int]]
    ):
        self.execute = cmd_execute_func

    @functools.cache
    def get_golang_tasks_info(self) -> tuple[dict[str, int], dict[str, int]]:
        data, err = self.execute(Py2GoCmd.CMD_GET_TASK_ACTOR_LIST, 0, b"")
        if err != 0:
            raise Exception(data.decode("utf-8"))
        return json.loads(data)

    @functools.cache
    def get_golang_actor_methods(self, actor_class_idx: int) -> dict[str, int]:
        data, err = self.execute(Py2GoCmd.CMD_GET_ACTOR_METHODS, actor_class_idx, b"")
        if err != 0:
            raise Exception(data.decode("utf-8"))
        return json.loads(data)

    def close_actor(self, go_instance_id: int) -> tuple[str, int]:
        res, code = self.execute(Py2GoCmd.CMD_CLOSE_ACTOR, go_instance_id, b"")
        return res.decode("utf8"), code

    def call_golang_func(self, func_name: str, args: tuple):
        raw_args = b"".join(msgpack.packb(arg, use_bin_type=True) for arg in args)
        res, code = self.raw_call_golang_func(func_name, raw_args, [])
        if code != 0:
            raise Exception(
                f"execute golang task {func_name} error: {res.decode('utf-8')}"
            )
        returns = list(msgpack.Unpacker(io.BytesIO(res)))
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
        low level api to call golang func, arguments and return values are encoded raw bytes
        """
        data, err = funccall.pack_golang_funccall_data(
            func_name, raw_args, object_positions, *resolved_object_refs
        )
        if err != 0:
            return data, err

        logger.debug(f"[py] local_run_task {func_name=}, {object_positions=}")
        try:
            res, code = self.execute(Py2GoCmd.CMD_RUN_TASK, 0, data)
        except Exception as e:
            logging.exception(f"[py] execute error {e}")
            return (
                f"[goray error] python ffi.execute() error: {e}".encode("utf-8"),
                ErrCode.Failed,
            )
        return res, code

    def start_driver(self) -> tuple[str, int]:
        data, code = self.execute(Py2GoCmd.CMD_START_DRIVER, 0, b"")
        return data.decode("utf8"), code
