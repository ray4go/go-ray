import inspect

import ray

from .. import x

# name -> (func_or_class, options)
_user_tasks = {}
_user_actors = {}


def make_remote(function_or_class, options: dict):
    if inspect.isclass(function_or_class):
        _user_actors[function_or_class.__name__] = (function_or_class, options)
    else:
        _user_tasks[function_or_class.__name__] = (function_or_class, options)

    if not inspect.isclass(function_or_class):
        # also register the function in x module for local cross-language calls
        # (local go-call-python only works for python functions yet)
        x.export(function_or_class)

    if options:
        return ray.remote(**options)(function_or_class)
    else:
        return ray.remote(function_or_class)


def get_py_task(name: str):
    return _user_tasks.get(name, (None, None))


def get_py_actor(name: str):
    return _user_actors.get(name, (None, None))


def all_py_tasks():
    return [i for i, _ in _user_tasks]


def all_py_actors():
    return [i for i, _ in _user_actors]
