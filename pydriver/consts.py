"""
和 go 中的 enum 对应
"""

import enum


class ErrCode(enum.IntEnum):
    Success = 0
    Failed = enum.auto()
    Timeout = enum.auto()
    Cancelled = enum.auto()


class Go2PyCmd(enum.IntEnum):
    CMD_INIT = 0
    CMD_EXECUTE_REMOTE_TASK = enum.auto()
    CMD_GET_OBJECT = enum.auto()
    CMD_PUT_OBJECT = enum.auto()
    CMD_WAIT_OBJECT = enum.auto()
    CMD_CANCEL_OBJECT = enum.auto()

    CMD_NEW_ACTOR = enum.auto()
    CMD_NEW_PY_ACTOR = enum.auto()
    CMD_ACTOR_METHOD_CALL = enum.auto()
    CMD_KILL_ACTOR = enum.auto()
    CMD_GET_ACTOR = enum.auto()

    CMD_EXECUTE_PY_REMOTE_TASK = enum.auto()
    CMD_EXECUTE_PY_LOCAL_TASK = enum.auto()

    CMD_EXECUTE_PYTHON_CODE = enum.auto()


class Py2GoCmd(enum.IntEnum):
    CMD_START_DRIVER = 0
    CMD_GET_INFO = enum.auto()
    CMD_RUN_TASK = enum.auto()
    CMD_NEW_ACTOR = enum.auto()
    CMD_ACTOR_METHOD_CALL = enum.auto()
