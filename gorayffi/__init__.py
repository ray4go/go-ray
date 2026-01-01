import traceback
from typing import Callable

from . import cmds, ffi, utils, consts


def load_go_lib(
    libpath: str, cmd_handlers: dict[int, Callable[[bytes], tuple[bytes, int]]]
) -> cmds.GoCommander:
    cmder = ffi.load_go_lib(libpath, _cmds_dispatcher(cmd_handlers))
    return cmds.GoCommander(cmder)


def _cmds_dispatcher(
    cmd2handler: dict[int, Callable[[bytes], tuple[bytes, int]]],
) -> Callable[[int, bytes], tuple[bytes, int]]:
    def handler(cmd: int, data: bytes) -> tuple[bytes, int]:
        if cmd not in cmd2handler:
            return utils.error_msg(f"Unknown Go2PyCmd {cmd}"), consts.ErrCode.Failed

        func = cmd2handler[cmd]
        try:
            return func(data)
        except Exception as e:
            error_string = (
                f"[python] handle command {consts.Go2PyCmd(cmd).name} error {e}\n"
                + traceback.format_exc()
            )
            return error_string.encode("utf8"), consts.ErrCode.Failed

    return handler
