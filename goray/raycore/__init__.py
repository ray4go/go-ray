import logging
import threading
import traceback

from . import actor, go2py, object, task
from ..consts import *
from ..x import handlers as cross_lang_handlers

handlers = {
    # Go2PyCmd.CMD_INIT: handle_init,
    # task
    Go2PyCmd.CMD_EXECUTE_REMOTE_TASK: task.handle_run_remote_task,
    # object
    Go2PyCmd.CMD_GET_OBJECT: object.handle_get_objects,
    Go2PyCmd.CMD_PUT_OBJECT: object.handle_put_object,
    Go2PyCmd.CMD_WAIT_OBJECT: object.handle_wait_object,
    Go2PyCmd.CMD_CANCEL_OBJECT: object.handle_cancel_object,
    # actor
    Go2PyCmd.CMD_NEW_ACTOR: actor.handle_new_actor,
    Go2PyCmd.CMD_KILL_ACTOR: actor.handle_kill_actor,
    Go2PyCmd.CMD_GET_ACTOR: actor.handle_get_actor,
    Go2PyCmd.CMD_ACTOR_METHOD_CALL: actor.handle_actor_method_call,
    # go call py
    Go2PyCmd.CMD_EXECUTE_PY_REMOTE_TASK: go2py.handle_run_py_task,
    Go2PyCmd.CMD_NEW_PY_ACTOR: go2py.handle_new_py_actor,
}

logger = logging.getLogger(__name__)


def handle(cmd: int, data: bytes) -> tuple[bytes, int]:
    if cmd in cross_lang_handlers.handlers:
        return cross_lang_handlers.handle(cmd, data)
    logger.debug(
        f"[py] handle {Go2PyCmd(cmd).name}, {len(data)=}, {threading.current_thread().name}"
    )
    func = handlers[cmd]
    try:
        return func(data)
    except Exception as e:
        error_string = (
            f"[python] handle {Go2PyCmd(cmd).name} error {e}\n" + traceback.format_exc()
        )
        # logger.error(error_string)
        return error_string.encode("utf8"), ErrCode.Failed
