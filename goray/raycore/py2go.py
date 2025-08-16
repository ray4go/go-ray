import logging

import ray

from .. import utils, x, consts
from . import common

logger = logging.getLogger(__name__)
utils.init_logger(logger)


def _run_golang_remote_task(func_name: str, *args):
    return common.load_go_lib().call_golang_func(func_name, args)


def get_golang_remote_task(name: str, options: dict) -> "GolangRemoteFunc":
    common.inject_runtime_env(options)
    remote_task = ray.remote(
        common.copy_function(_run_golang_remote_task, name, consts.Language.GO)
    )
    return GolangRemoteFunc(remote_task, name, **options)


class GolangRemoteFunc:
    """
    GolangRemoteFunc represents a remote task or actor method.
    The usage is same as @ray.remote decorated function or actor.method.
    """

    def __init__(self, remote_callable_handle, bind_arg, **options):
        self._remote_handle = remote_callable_handle
        self._bind_arg = bind_arg
        self._options = options

    def options(self, **kwargs) -> "GolangRemoteFunc":
        options = dict(self._options)
        options.update(kwargs)
        # todo: make sure user didn't overwrite runtime_env/env_vars
        return GolangRemoteFunc(self._remote_handle, self._bind_arg, **options)

    def remote(self, *args):
        return self._remote_handle.options(**self._options).remote(
            self._bind_arg, *args
        )


class _RemoteActor:
    def __init__(self, actor_class_name: str, *args):
        cmder = common.load_go_lib()
        self._actor = x.GolangLocalActor(cmder, actor_class_name, *args)

    def call_method(self, method_name: str, *args):
        return self._actor._call_method(method_name, *args)


class GolangRemoteActorHandle:
    """
    The usage is same as ray actor handle.
    """

    def __init__(self, actor_handle: _RemoteActor):
        self._actor = actor_handle

    def __getattr__(self, method_name: str) -> "GolangRemoteFunc":
        return GolangRemoteFunc(self._actor.call_method, method_name)


class GolangActorClass:
    """
    The usage is same as @ray.remote decorated class.
    """

    def __init__(self, class_name: str, **options):
        self._class_name = class_name
        self._options = options

    def options(self, **kwargs) -> "GolangActorClass":
        options = dict(self._options)
        options.update(kwargs)
        return GolangActorClass(self._class_name, **options)

    def remote(self, *args) -> "GolangRemoteActorHandle":
        tasks_name2idx, actors_name2idx = common.load_go_lib().get_golang_tasks_info()
        if self._class_name not in actors_name2idx:
            raise Exception(f"golang actor {self._class_name} not found")
        common.inject_runtime_env(self._options)

        ActorCls = ray.remote(common.copy_class(_RemoteActor, self._class_name, "Go"))
        actor_handle = ActorCls.options(**self._options).remote(self._class_name, *args)
        return GolangRemoteActorHandle(actor_handle)
