from gorayffi import handlers as cross_lang_handlers
from gorayffi.consts import *
from . import actor, go2py, object, task

handlers = {
    # task
    Go2PyCmd.CMD_EXECUTE_REMOTE_TASK: task.handle_run_remote_task,
    # object
    Go2PyCmd.CMD_GET_OBJECT: object.handle_get_objects,
    Go2PyCmd.CMD_PUT_OBJECT: object.handle_put_object,
    Go2PyCmd.CMD_WAIT_OBJECT: object.handle_wait_object,
    Go2PyCmd.CMD_CANCEL_OBJECT: object.handle_cancel_object,
    Go2PyCmd.CMD_RELEASE_OBJECT: object.handle_release_object,
    # actor
    Go2PyCmd.CMD_NEW_ACTOR: actor.handle_new_actor,
    Go2PyCmd.CMD_KILL_ACTOR: actor.handle_kill_actor,
    Go2PyCmd.CMD_GET_ACTOR: actor.handle_get_actor,
    Go2PyCmd.CMD_ACTOR_METHOD_CALL: actor.handle_actor_method_call,
    # go call py
    Go2PyCmd.CMD_EXECUTE_PY_REMOTE_TASK: go2py.handle_run_py_task,
    Go2PyCmd.CMD_NEW_PY_ACTOR: go2py.handle_new_py_actor,

    # local call
    **cross_lang_handlers.handlers,
}
