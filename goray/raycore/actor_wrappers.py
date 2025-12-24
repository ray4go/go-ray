import logging
from typing import Any, Union, Type

import io
import typing
import msgpack
import ray

from . import common
from . import registry
from gorayffi.consts import *
from gorayffi import actor

logger = logging.getLogger(__name__)



class GoActor(actor.GoActor):
    """
    Go remote actor created from go/python, called from go/python
    """

    # the constructor is call by ray framework
    def __init__(
        self,
        actor_class_name: str,
        caller_type: actor.CallerLang,  # create actor from golang or python
        go_encoded_args: bytes,  # set when caller_type=golang
        go_object_positions: list[int],  # set when caller_type=golang
        # when set when caller_type=golang, it's go_encoded_object_refs; when python, it's py_native_args
        *go_encoded_object_refs_or_py_native_args: Union[tuple[bytes, int], Any],
    ):
        super().__init__(
            common.load_go_lib(),
            actor_class_name,
            caller_type,
            go_encoded_args,
            go_object_positions,
            *go_encoded_object_refs_or_py_native_args,
        )


class PyActor:
    """
    Python actor created from go, called from go/python
    """

    # the constructor is call by ray framework
    def __init__(
        self,
        actor_class_name: str,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ):
        common.load_go_lib()

        cls, opts = registry.get_py_actor(actor_class_name)
        if cls is None:
            raise Exception(
                f"python actor {actor_class_name} not found, all py actors: {registry.all_py_actors()}"
            )

        args = common.decode_args(raw_args, object_positions, object_refs)
        self._instance = cls(*args)

    # args and returns is go msgpack-encoded
    # used for go calling
    def call_method_with_encoded_args(
        self,
        method_name: str,
        encoded_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
    ) -> tuple[bytes, int]:
        args = common.decode_args(encoded_args, object_positions, object_refs)
        try:
            res = getattr(self._instance, method_name)(*args)
        except Exception as e:
            logging.exception(f"[py] execute error {e}")
            return (
                f"[goray error] python run task error: {e}".encode("utf-8"),
                ErrCode.Failed,
            )
        return msgpack.packb(res, use_bin_type=True), ErrCode.Success

    # args and returns is python native
    # used for python calling
    def call_method_with_native_args(
        self,
        __method_name__: str,  # to avoid conflict with kwargs
        *args,
        **kwargs,
    ) -> Any:
        try:
            method = getattr(self._instance, __method_name__)
        except AttributeError:
            raise AttributeError(
                f"Method {__method_name__} not found in actor {self._instance}"
            )

        return method(*args, **kwargs)

    # func_name__ = call_method_with_encoded_args
    # func_name = call_method_with_native_args(method_name)


METHOD_WITH_ENCODED_ARGS_SUFFIX = "__"

def new_remote_actor_type(
    cls: Union[Type[GoActor], Type[PyActor]],
    actor_type_name: str,
    method_names: list[str],
    namespace: str,
):
    go_methods = {
        name + METHOD_WITH_ENCODED_ARGS_SUFFIX: cls.call_method_with_encoded_args for name in method_names
    }
    py_method = {
        name: common.method_bind(cls.call_method_with_native_args, name)
        for name in method_names
    }
    ActorCls = ray.remote(
        common.copy_class(
            cls, actor_type_name, namespace=namespace, **go_methods, **py_method
        )
    )
    return ActorCls


class PyNativeActorWrapper:
    """
    Actor wrapper for python native actor (@ray.remote, not @goray.remote), and call from go.

    This object will not be seen by ray.
    """

    def __init__(self, actor):
        self._actor = actor

    # args and returns is go msgpack-encoded
    # used for go calling
    def call_method_with_encoded_args(
        self,
        method_name: str,
        encoded_args: bytes,
        object_positions: list[int],
        *object_refs: tuple[bytes, int],
        ray_options: dict = {},
    ) -> "ray.types.ObjectRef":
        try:
            remote_method = getattr(self._actor, method_name)
        except AttributeError:
            raise AttributeError(
                f"Method {method_name} not found in actor {self._actor}"
            )
        args = common.decode_args(encoded_args, object_positions, object_refs)
        return remote_method.options(ray_options).remote(*args)
