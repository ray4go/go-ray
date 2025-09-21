import enum
import io
import logging
from typing import Any

import msgpack

from . import cmds
from .. import funccall, utils, consts

logger = logging.getLogger(__name__)


class CallerLang(enum.Enum):
    Golang = "golang"
    Python = "python"


def _encode_native_args(*args) -> bytes:
    return b"".join(msgpack.packb(arg, use_bin_type=True) for arg in args)


class GoActor:
    """
    Go actor created from go/python, called from go/python
    """

    cmder: cmds.GoCommander
    go_instance_index: int

    # the constructor is call by ray framework
    def __init__(
        self,
        cmder: cmds.GoCommander,
        actor_class_name: str,
        caller_type: CallerLang,  # create actor from golang or python
        go_encoded_args: bytes,  # set when caller_type=golang
        go_object_positions: list[int],  # set when caller_type=golang
        # when set when caller_type=golang, it's go_encoded_object_refs; when python, it's py_native_args
        *go_encoded_object_refs_or_py_native_args: tuple[bytes, int] | Any,
    ):
        self.cmder = cmder

        if caller_type == CallerLang.Python:
            go_encoded_args = _encode_native_args(
                *go_encoded_object_refs_or_py_native_args
            )
            go_object_positions = []
            encoded_object_refs = []
        else:
            encoded_object_refs = go_encoded_object_refs_or_py_native_args

        data, err = funccall.pack_golang_funccall_data(
            actor_class_name,
            go_encoded_args,
            go_object_positions,
            *encoded_object_refs,
        )
        if err != 0:
            raise Exception(data.decode("utf-8"))
        logger.debug(f"[py] new golang actor {actor_class_name}")

        res, code = cmder.execute(consts.Py2GoCmd.CMD_NEW_ACTOR, 0, data)
        if code != consts.ErrCode.Success:
            raise Exception("create golang actor error: " + res.decode("utf-8"))

        self.go_instance_index = int(res.decode("utf-8"))

    # args and returns is go msgpack-encoded
    # used for go calling
    def call_method_with_encoded_args(
        self,
        method_name: str,
        encoded_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ) -> tuple[bytes, int]:
        data, err = funccall.pack_golang_funccall_data(
            method_name, encoded_args, object_positions, *object_refs
        )
        if err != 0:
            return data, err

        logger.debug(
            f"[py] run actor method {method_name=}, {self.go_instance_index =}"
        )
        try:
            res, code = self.cmder.execute(
                consts.Py2GoCmd.CMD_ACTOR_METHOD_CALL, self.go_instance_index, data
            )
        except Exception as e:
            logging.exception(f"[py] execute actor method error {e}")
            return (
                utils.error_msg(f"execute actor method error: {e}"),
                consts.ErrCode.Failed,
            )
        return res, code

    # args and returns is python native
    # used for python calling
    def call_method_with_native_args(
        self,
        method_name: str,
        *args,
    ) -> Any:
        encoded_args = _encode_native_args(*args)
        res, code = self.call_method_with_encoded_args(method_name, encoded_args, [])
        if code != consts.ErrCode.Success:
            raise Exception(
                f"Run golang actor method {method_name} error: {res.decode('utf-8')}"
            )
        returns = list(msgpack.Unpacker(io.BytesIO(res), strict_map_key=False))
        if len(returns) == 1:
            return returns[0]
        if len(returns) == 0:
            return None
        return returns

    # func_name__ = call_method_with_encoded_args
    # func_name = call_method_with_native_args(method_name)
