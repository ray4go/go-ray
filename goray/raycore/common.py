from .. import consts, state, utils
from ..x import ffi


def load_go_lib():
    global _loaded_libs
    globals().setdefault("_loaded_libs", False)
    from . import handle

    if _loaded_libs:
        return _loaded_libs

    if not state.golibpath:
        raise Exception("init() must be called first")

    lib = ffi.load_go_lib(state.golibpath, handle)

    if state.pymodulepath:
        utils.get_module(state.pymodulepath)

    _loaded_libs = lib
    return lib


def inject_runtime_env(options: dict):
    if options.get("num_returns", 1) > 1:
        raise Exception("num_returns is not supported in goray")

    options.setdefault("runtime_env", {})
    options["runtime_env"].setdefault("env_vars", {})
    options["runtime_env"]["env_vars"][consts.GORAY_BIN_PATH_ENV] = state.golibpath
    options["runtime_env"]["env_vars"][
        consts.GORAY_PY_MUDULE_PATH_ENV
    ] = state.pymodulepath


def copy_function(func, name: str, namespace: str = ""):
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    wrapper.__name__ = name
    if namespace:
        namespace = namespace + "."
    wrapper.__qualname__ = namespace + name
    wrapper.__module__ = "goray"
    return wrapper


def copy_class(cls, name: str, namespace: str = "", **members):
    if namespace:
        namespace = namespace + "."

    members.update(
        {"__module__": "goray", "__qualname__": namespace + name, "__name__": name}
    )
    new_cls = type(
        name,
        (cls,),
        members,
    )
    return new_cls


def get_class_methods(cls):
    return [
        func
        for func in dir(cls)
        if callable(getattr(cls, func)) and not func.startswith("__")
    ]
