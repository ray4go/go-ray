import enum

class ErrCode(enum.IntEnum):
    Success = 0
    Failed  = enum.auto()
    Timeout = enum.auto()