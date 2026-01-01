from gorayffi import handlers as cross_lang_handlers, consts
from . import actor, go2py, object, task, registry

handlers = {
    # task
    consts.Go2PyCmd.CMD_EXECUTE_REMOTE_TASK: task.handle_run_remote_task,
    # object
    consts.Go2PyCmd.CMD_GET_OBJECT: object.handle_get_objects,
    consts.Go2PyCmd.CMD_PUT_OBJECT: object.handle_put_object,
    consts.Go2PyCmd.CMD_WAIT_OBJECT: object.handle_wait_object,
    consts.Go2PyCmd.CMD_CANCEL_OBJECT: object.handle_cancel_object,
    consts.Go2PyCmd.CMD_RELEASE_OBJECT: object.handle_release_object,
    # actor
    consts.Go2PyCmd.CMD_NEW_ACTOR: actor.handle_new_actor,
    consts.Go2PyCmd.CMD_KILL_ACTOR: actor.handle_kill_actor,
    consts.Go2PyCmd.CMD_GET_ACTOR: actor.handle_get_actor,
    consts.Go2PyCmd.CMD_ACTOR_METHOD_CALL: actor.handle_actor_method_call,
    # go call py
    consts.Go2PyCmd.CMD_EXECUTE_PY_REMOTE_TASK: go2py.handle_run_py_task,
    consts.Go2PyCmd.CMD_NEW_PY_ACTOR: go2py.handle_new_py_actor,
    # local call
    **cross_lang_handlers.get_handlers(
        python_func_getter=registry.local_funcs.get,
        python_class_getter=registry.local_classes.get,
    ),
}
