import collections
import contextlib
import importlib.util
import logging
import os
import threading
import traceback

GORAY_DEBUG_LOGGING = "GORAY_DEBUG_LOGGING"


def enable_logger(logger, level=logging.DEBUG):
    """Output logging message to sys.stderr"""
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = logging.Formatter(
        "[%(levelname)s %(asctime)s %(module)s:%(lineno)d %(funcName)s] %(message)s",
        datefmt="%y%m%d %H:%M:%S",
    )
    ch.setFormatter(formatter)
    logger.handlers = [ch]
    logger.setLevel(level)
    logger.propagate = False


def init_logger(logger):
    if os.getenv(GORAY_DEBUG_LOGGING):
        enable_logger(logger, level=logging.DEBUG)


def pack_bytes_units(data: list[bytes]) -> bytes:
    return b"".join([len(d).to_bytes(8, byteorder="little") + d for d in data])


def unpack_bytes_units(data: bytes) -> list[bytes]:
    offset = 0
    units = []
    while offset < len(data):
        length = int.from_bytes(data[offset : offset + 8], byteorder="little")
        offset += 8
        units.append(data[offset : offset + length])
        offset += length
    assert offset == len(
        data
    ), f"unpack_bytes_units failed, read finish with {offset=} while {len(data)=}"
    return units


class ThreadSafeLocalStore:
    """
    A k-v store.
    It's thread safe.
    It's a local store, meaning that when pickle and unpickle this object, the store will be reset.

    When add a new value, it will return an integer key. The key is assigned in a round-robi



    n fashion.
    The released key will not be reused in right away, just like the process id generation strategy.
    """

    MAX_SIZE = 2**63 - 1

    _no_set = object()

    def __init__(self):
        self._write_lock = threading.Lock()
        self._kvstore = {}
        self._reused_keys = (
            collections.deque()
        )  # when reused_keys is not empty, it will be used first to assign a new key

    def __getstate__(self):
        """used by pickle to serialize the object"""
        return {}

    def __setstate__(self, state):
        """used by pickle to deserialize the object"""
        if state:
            logging.warning(
                f"ThreadSafeLocalStore should not be serialized with {state=}, it's a bug in goray"
            )
        self._write_lock = threading.Lock()
        self._kvstore = {}
        return

    def __contains__(self, key: int):
        return (
            key in self._kvstore and self._kvstore[key] != ThreadSafeLocalStore._no_set
        )

    def __getitem__(self, key: int):
        val = self._kvstore[key]
        if val == ThreadSafeLocalStore._no_set:
            raise KeyError(key)
        return val

    def add(self, value) -> int:
        assert value is not None, "value can't be None"
        with self._write_lock:
            if len(self._reused_keys):
                key = self._reused_keys.popleft()
            else:
                key = len(self._kvstore)
            self._kvstore[key] = value
            if len(self._kvstore) > ThreadSafeLocalStore.MAX_SIZE:
                self._reuse_keys()
            return key

    def _reuse_keys(self):
        """remove _no_set value and release it's key for reuse"""
        for k in list(self._kvstore.keys()):
            if self._kvstore[k] == ThreadSafeLocalStore._no_set:
                self._kvstore.pop(k)
                self._reused_keys.append(k)

    def release(self, key: int):
        with self._write_lock:
            self._kvstore[key] = ThreadSafeLocalStore._no_set


def _get_caller_info(frame_index):
    """
    获取当前调用栈中上方第 frame_index 个调用的模块名和行号。

    frame_index=0: 当前函数本身
    frame_index=1: 调用当前函数的函数
    frame_index=2: 调用调用当前函数的函数
    以此类推...
    Returns:
        tuple: (module_fname, line_number) 或 ("", -1) 如果索引超出范围。
    """
    try:
        stack_frames = traceback.extract_stack()

        # stack_frames[-1] 是 get_caller_info 函数调用本身
        # stack_frames[-2] 是调用 get_caller_info 的函数

        if -(frame_index + 2) >= -len(stack_frames):
            frame_info = stack_frames[-(frame_index + 2)]
            # frame_info 是一个 ExtractStack 命名元组 (filename, lineno, name, line)
            filename = frame_info.filename
            lineno = frame_info.lineno
            # 从文件名中提取模块名
            return os.path.basename(filename), lineno
        else:
            return "", -1
    except Exception as e:
        return "", -1


def error_msg(msg: str, frame_index: int = 0) -> bytes:
    """
    Get error message with caller info.
    :param frame_index:
        frame_index=0: 当前函数本身
        frame_index=1: 调用当前函数的函数
        frame_index=2: 调用调用当前函数的函数
    """
    fname, lineno = _get_caller_info(frame_index + 1)
    return f"[ERROR]@{fname}:{lineno} {msg} ".encode("utf-8")


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
