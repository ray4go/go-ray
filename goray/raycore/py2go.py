import functools
import inspect
import logging

import ray

from . import common
from .. import utils
from .. import x

logger = logging.getLogger(__name__)
utils.init_logger(logger)


@ray.remote
def _golang_remote_task(func_id: int, *args):
    return common.load_go_lib().call_golang_func(func_id, args)


def golang_task(name: str, options:dict) -> "GolangRemoteFunc":
    tasks_name2idx, actors_name2idx = common.load_go_lib().get_golang_tasks_info()
    func_id = tasks_name2idx[name]
    return GolangRemoteFunc(_golang_remote_task, func_id, **options)


class GolangRemoteFunc:
    def __init__(self, remote_callable_handle, bind_arg, **options):
        self._remote_handle = remote_callable_handle
        self._bind_arg = bind_arg
        self._options = options

    def options(self, **kwargs) -> "GolangRemoteFunc":
        options = dict(self._options)
        options.update(kwargs)
        return GolangRemoteFunc(self._remote_handle, self._bind_arg, **options)

    def remote(self, *args):
        common.inject_runtime_env(self._options)
        return self._remote_handle.options(**self._options).remote(
            self._bind_arg, *args
        )


@ray.remote
class _RemoteActor:
    def __init__(self, actor_class_name: str, *args):
        cmder = common.load_go_lib()
        self._actor = x.GolangLocalActor(cmder, actor_class_name, *args)

    def call_method(self, method_name: str, *args):
        return self._actor._call_method(method_name, *args)


class GolangRemoteActorHandle:
    def __init__(self, actor_handle: _RemoteActor):
        self._actor = actor_handle

    def __getattr__(self, method_name: str) -> "GolangRemoteFunc":
        return GolangRemoteFunc(self._actor.call_method, method_name)


class GolangActorClass:
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
        actor_handle = _RemoteActor.options(**self._options).remote(
            self._class_name, *args
        )
        return GolangRemoteActorHandle(actor_handle)
