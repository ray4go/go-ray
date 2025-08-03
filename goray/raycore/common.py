from .. import state
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
    _loaded_libs = lib
    return lib
