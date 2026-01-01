import enum
import functools
import io
import logging
import typing
from typing import Any, Union

import msgpack

from . import cmds
from . import consts

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
        *go_encoded_object_refs_or_py_native_args: Union[tuple[bytes, int], Any],
    ):
        self.cmder = cmder

        encoded_object_refs: list[tuple[bytes, int]] = []
        if caller_type == CallerLang.Python:
            go_encoded_args = _encode_native_args(
                *go_encoded_object_refs_or_py_native_args
            )
            go_object_positions = []
        else:
            encoded_object_refs = go_encoded_object_refs_or_py_native_args  # type: ignore

        self.go_instance_index = self.cmder.new_golang_actor(
            actor_class_name,
            go_encoded_args,
            go_object_positions,
            *encoded_object_refs,
        )

    # args and returns is go msgpack-encoded
    # used for go calling
    def call_method_with_encoded_args(
        self,
        method_name: str,
        encoded_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ) -> tuple[bytes, int]:
        return self.cmder.raw_call_golang_method(
            self.go_instance_index,
            method_name,
            encoded_args,
            object_positions,
            *object_refs,
        )

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


class GolangLocalActor:
    """
    Golang actor wrapper for python local (in-process) call.
    """

    def __init__(
        self,
        cmder: cmds.GoCommander,
        actor_class_name: str,
        *args,
    ):
        self._cmder = cmder
        self._actor_class_name = actor_class_name

        self._method_names = self._cmder.get_golang_actor_methods(actor_class_name)
        self._actor = GoActor(
            self._cmder,
            actor_class_name,
            CallerLang.Python,
            b"",
            [],
            *args,
        )

    def __getattr__(self, name) -> typing.Callable:
        if name not in self._method_names:
            raise AttributeError(
                f"golang actor type {self._actor_class_name!r} has no method {name!r}"
            )
        return functools.partial(self._actor.call_method_with_native_args, name)

    def __repr__(self):
        return f"<GolangLocalActor {self._actor_class_name} id={self._actor.go_instance_index}>"

    def __del__(self):
        if not hasattr(self, "_actor"):
            return
        msg, code = self._cmder.close_actor(self._actor.go_instance_index)
        if code != 0:
            logging.error(f"close actor {self._actor_class_name} error: {msg}")
