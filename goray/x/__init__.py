# x for cross language
import functools
import logging
import typing

from . import actor, cmds, ffi, handlers


class GolangLocalActor:
    """
    Golang actor wrapper for local (in-process) call.
    """

    def __init__(
        self,
        cmder: cmds.GoCommander,
        actor_class_name: str,
        method_names: list[str],
        *args,
    ):
        self._cmder = cmder
        self._actor_class_name = actor_class_name
        self._actor = None

        self._method_names = method_names
        self._actor = actor.GoActor(
            self._cmder,
            actor_class_name,
            actor.CallerLang.Python,
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
        if self._actor is None:
            return
        msg, code = self._cmder.close_actor(self._actor.go_instance_index)
        if code != 0:
            logging.error(f"close actor {self._actor_class_name} error: {msg}")


class CrossLanguageClient:
    def __init__(self, cmder: cmds.GoCommander):
        self._cmder = cmder

    def start_driver(self):
        self._cmder.start_driver()

    def func_call(self, func_name: str, *args):
        return self._cmder.call_golang_func(func_name, args)

    def new_type(self, type_name: str, *args):
        method_names = self._cmder.get_golang_actor_methods(type_name)
        return GolangLocalActor(self._cmder, type_name, method_names, *args)


@functools.cache
def load_go_lib(libpath: str) -> CrossLanguageClient:
    cmder = ffi.load_go_lib(libpath, handlers.handle)
    return CrossLanguageClient(cmder)


def export(func):
    """
    Register a python function to be called from go.

    Usage:
    @export
    def my_func(arg1, arg2):
        ...
    """
    handlers.add_python_export_func(func)
    return func
