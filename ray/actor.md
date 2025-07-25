
## 约定


bytes unit format: | length:8byte:int64 | data:${length}byte:[]byte |

## Task

```python
@ray.remote
def ray_run_task(
    func_id: int,
    raw_args: bytes,
    object_positions: list[int],
    *object_refs: list[tuple[bytes, int]],
) -> tuple[bytes, int]:
    """
    :param func_id:
    :param data:
    :param object_positions: 调用的go task func中, 传入的object_ref作为参数的位置列表, 有序
    :param object_refs: object_ref列表, 会被ray core替换为实际的数据
    :return:
    """
    init_ffi_once()
    return run_task(func_id, raw_args, object_positions, *object_refs)
```


### remote task call

* go -> python: ExeRemoteTask
  | cmdId   | taskIndex | 
  | 10 bits | 54 bits   |
  data: arguments_bytes + options_bytes

* python:
- call remote task
- save objectref to local store
- return objectref_local_id

* remote python -> go: RunTask
| cmdId   | taskIndex |
| 10 bits | 54 bits   |

data: raw_args_data + objectRefs_resolved_data...
- resolved data format: | arg_pos:8byte:int64 | data:[]byte |

### get object

* go -> python: GetObject
data: json [py_objectref_local_id, timeout]

## Actor

```python
@ray.remote
class Actor:
    def __init__(
        self,
        actor_class_idx: int,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: list[tuple[bytes, int]],
    ):
        pass

    def method(
        self,
        method_idx: int,
        raw_args: bytes,
        object_positions: list[int],
        *object_refs: list[tuple[bytes, int]],
    ) -> tuple[bytes, int]:
        pass

```
由于 actor实例上，所有方法串行执行，因此只需要定义一个方法即可

### New actor

* go -> python: new_actor
  | cmdId   | actorIndex |
  | 10 bits | 54 bits    |
  data: arguments_bytes + options_bytes

* python:
- init an actor handle
- save actor to local store
- return actor_local_id 

* remote python -> go: new_actor
| cmdId   | actorIndex |
| 10 bits | 54 bits    |

data format: multiple bytes units
- first unit is raw args data;
- other units are objectRefs resolved data;
    - resolved data format: | arg_pos:8byte:int64 | data:[]byte |

return actorGoInstanceIndex

* remote python:
 - save actorGoInstanceIndex to instance statues

### actor method call

* go -> python: actor_call
  | cmdId   | methodIndex | PyActorId |
  | 10 bits | 22 bits     |  32 bits  |
  data: arguments_bytes + options_bytes

* python:
- get actor handle from actorPyLocalId
- call actor method
- save objectref to local store
- return objectref_local_id

* remote python -> go: actor_method_call
  | cmdId   | methodIndex |  actorGoInstanceIndex |
  | 10 bits | 22 bits     |  32 bits              |

data format: multiple bytes units
- first unit is raw args data;
- other units are objectRefs resolved data;
  - resolved data format: | arg_pos:8byte:int64 | data:[]byte |

### kill

* go -> python: kill_call
  | cmdId   | PyActorId |
  | 10 bits | 54 bits   |
  data: options_bytes

* python:
- get actor handle from actorPyLocalId
- kill actor
- return 0


### Get actor

* go -> python: get_actor
  | cmdId   | empty   |
  | 10 bits | 54 bits |
  data: options_bytes

* python:
- ray.get_actor(**options)
- save actor to local store
- query actorIndex from actor.
- return actor_local_id



// todo 指定类型，应用场景，获取之前提交的detached task
----


