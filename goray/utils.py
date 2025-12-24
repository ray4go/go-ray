import contextlib
import functools
import importlib.util
import os


@functools.cache
def get_module(path):
    # https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly
    # https://stackoverflow.com/questions/41861427/python-3-5-how-to-dynamically-import-a-module-given-the-full-file-path-in-the

    @contextlib.contextmanager
    def add_to_path(p):
        import sys

        sys.path.append(p)
        try:
            yield
        finally:
            sys.path.remove(p)

    # import_name will be the `__name__` of the imported module
    import_name = "__goray__"
    with add_to_path(os.path.dirname(path)):
        spec = importlib.util.spec_from_file_location(
            import_name, path, submodule_search_locations=None
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module
