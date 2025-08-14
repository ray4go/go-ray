import logging

from .. import funccall
from ..consts import *
from . import cmds

logger = logging.getLogger(__name__)


class GoActor:
    cmder: cmds.GoCommander
    go_instance_index: int
    go_class_idx: int

    def __init__(
        self,
        cmder: cmds.GoCommander,
        actor_class_idx: int,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ):
        self.cmder = cmder

        data, err = funccall.pack_golang_funccall_data(
            raw_args, object_positions, *object_refs
        )
        if err != 0:
            raise Exception(data.decode("utf-8"))

        res, code = cmder.execute(Py2GoCmd.CMD_NEW_ACTOR, actor_class_idx, data)
        logger.debug(f"[py] CMD_NEW_ACTOR {actor_class_idx=}, {res=} {code=}")
        if code != ErrCode.Success:
            raise Exception("go ffi.execute failed: " + res.decode("utf-8"))

        self.go_instance_index = int(res.decode("utf-8"))
        self.go_class_idx = actor_class_idx

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

        header = method_idx | self.go_instance_index << 22
        try:
            res, code = self.cmder.execute(Py2GoCmd.CMD_ACTOR_METHOD_CALL, header, data)
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
