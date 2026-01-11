import inspect

import ray

# name -> (func_or_class, options)
_remote_tasks = {}
_remote_actors = {}


def make_remote(function_or_class, options: dict):
    if inspect.isclass(function_or_class):
        _remote_actors[function_or_class.__name__] = (function_or_class, options)
    else:
        _remote_tasks[function_or_class.__name__] = (function_or_class, options)

    make_local(function_or_class)  # also register it as local

    if options:
        return ray.remote(**options)(function_or_class)
    else:
        return ray.remote(function_or_class)


def get_py_task(name: str):
    return _remote_tasks.get(name, (None, None))


def get_py_actor(name: str):
    return _remote_actors.get(name, (None, None))


def all_py_tasks():
    return list(_remote_tasks)


def all_py_actors():
    return list(_remote_actors)


local_funcs = {}
local_classes = {}


def make_local(function_or_class):
    if inspect.isclass(function_or_class):
        local_classes[function_or_class.__name__] = function_or_class
    else:
        local_funcs[function_or_class.__name__] = function_or_class
    return function_or_class
