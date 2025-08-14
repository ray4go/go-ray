import inspect

import ray

from .. import x

# name -> (func_or_class, options)
_user_tasks_actors = {}


def make_remote(function_or_class, options: dict):
    _user_tasks_actors[function_or_class.__name__] = (function_or_class, options)
    if not inspect.isclass(function_or_class):
        x.export(function_or_class)

    if options:
        return ray.remote(**options)(function_or_class)
    else:
        return ray.remote(function_or_class)


# todo: use separate map for actors and tasks
def get_user_tasks_or_actors(name: str):
    return _user_tasks_actors.get(name)


def all_user_tasks_or_actors():
    return _user_tasks_actors