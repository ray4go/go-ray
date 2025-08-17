import logging

from .. import funccall, utils
from ..consts import *
from . import cmds

logger = logging.getLogger(__name__)


class GoActorWrapper:
    """Go actor wrapper for python remote call."""

    cmder: cmds.GoCommander
    go_instance_index: int

    def __init__(
        self,
        cmder: cmds.GoCommander,
        actor_class_name: str,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ):
        self.cmder = cmder

        data, err = funccall.pack_golang_funccall_data(
            actor_class_name, raw_args, object_positions, *object_refs
        )
        if err != 0:
            raise Exception(data.decode("utf-8"))
        logger.debug(f"[py] new golang actor {actor_class_name}")

        res, code = cmder.execute(Py2GoCmd.CMD_NEW_ACTOR, 0, data)
        if code != ErrCode.Success:
            raise Exception("create golang actor error: " + res.decode("utf-8"))

        self.go_instance_index = int(res.decode("utf-8"))

    def call_method(
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

        logger.debug(
            f"[py] run actor method {method_name=}, {self.go_instance_index =}"
        )
        try:
            res, code = self.cmder.execute(
                Py2GoCmd.CMD_ACTOR_METHOD_CALL, self.go_instance_index, data
            )
        except Exception as e:
            logging.exception(f"[py] execute actor method error {e}")
            return (
                utils.error_msg(f"execute actor method error: {e}"),
                ErrCode.Failed,
            )
        return res, code
