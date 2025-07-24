import logging
import os
import threading

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
    assert offset == len(data), (
        f"unpack_bytes_units failed, read finish with {offset=} while {len(data)=}"
    )
    return units

class ThreadSafeLocalStore:
    """
    It's thread safe.
    It's a local store, meaning that when pickle and unpickle this object, the store will be reset.
    """

    def __init__(self):
        self._write_lock = threading.Lock()
        self._store = {}

    def __getstate__(self):
        """used by pickle to serialize the object"""
        return self._store

    def __setstate__(self, state):
        """used by pickle to deserialize the object"""
        # todo: figure out this
        # if state:
        #     logging.warning(
        #         f"ThreadSafeLocalStore should not be serialized with {state=}, it's a bug in goray"
        #     )
        self._write_lock = threading.Lock()
        self._store = {}
        return

    def __contains__(self, key: int):
        return key in self._store

    def __getitem__(self, key: int):
        return self._store[key]

    def add(self, value) -> int:
        with self._write_lock:
            key = len(self._store)
            self._store[key] = value
            return key

    def pop(self, key: int):
        with self._write_lock:
            return self._store.pop(key, None)


