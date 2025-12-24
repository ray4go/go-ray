import inspect

# name to function
_python_export_funcs = {}
_python_export_classes = {}


def export_python(cls_or_func) -> bool:
    """
    Export a python function or class to be callable from golang.
    """
    global _python_export_classes, _python_export_funcs

    if inspect.isfunction(cls_or_func):
        _python_export_funcs[cls_or_func.__name__] = cls_or_func
        return True

    if inspect.isclass(cls_or_func):
        _python_export_classes[cls_or_func.__name__] = cls_or_func
        return True

    return False


def get_export_python_func(func_name: str):
    return _python_export_funcs.get(func_name)


def get_export_python_class(cls_name: str):
    return _python_export_classes.get(cls_name)
