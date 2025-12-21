"""
和 go 中的 enum 对应
"""

import enum


class ErrCode(enum.IntEnum):
    Success = 0
    Failed = enum.auto()
    Timeout = enum.auto()
    Cancelled = enum.auto()
    ObjectRefNotFound = enum.auto()


class Go2PyCmd(enum.IntEnum):
    CMD_EXECUTE_REMOTE_TASK = 0
    CMD_GET_OBJECT = enum.auto()
    CMD_PUT_OBJECT = enum.auto()
    CMD_WAIT_OBJECT = enum.auto()
    CMD_CANCEL_OBJECT = enum.auto()
    CMD_RELEASE_OBJECT = enum.auto()

    CMD_NEW_ACTOR = enum.auto()
    CMD_NEW_PY_ACTOR = enum.auto()
    CMD_ACTOR_METHOD_CALL = enum.auto()
    CMD_KILL_ACTOR = enum.auto()
    CMD_GET_ACTOR = enum.auto()

    CMD_EXECUTE_PY_REMOTE_TASK = enum.auto()
    CMD_EXECUTE_PY_LOCAL_TASK = enum.auto()

    CMD_EXECUTE_PYTHON_CODE = enum.auto()


class Py2GoCmd(enum.IntEnum):
    CMD_GET_INIT_OPTIONS = 0
    CMD_START_DRIVER = enum.auto()
    CMD_GET_TASK_ACTOR_LIST = enum.auto()
    CMD_GET_ACTOR_METHODS = enum.auto()

    CMD_RUN_TASK = enum.auto()
    CMD_NEW_ACTOR = enum.auto()
    CMD_ACTOR_METHOD_CALL = enum.auto()
    CMD_CLOSE_ACTOR = enum.auto()


# enum.StrEnum need py3.11+, so we use a simple class instead
class ActorSourceLang:
    GO = "Go"  # implemented in go
    PY = "PY"  # implemented in python


GORAY_BIN_PATH_ENV = "GORAY_BIN_PATH"
GORAY_PY_MUDULE_PATH_ENV = "GORAY_PY_MUDULE_PATH"

PY_LOCAL_ACTOR_ID_KEY = "goray_py_local_actor_id"
TASK_NAME_OPTION_KEY = "goray_task_name"
ACTOR_NAME_OPTION_KEY = "goray_actor_type_name"
ACTOR_METHOD_LIST_OPTION_KEY = "goray_actor_methods"
