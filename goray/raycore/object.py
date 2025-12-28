import json
import logging

import ray

from gorayffi import consts
from . import common
from .. import state

logger = logging.getLogger(__name__)


def handle_get_objects(data: bytes) -> tuple[bytes, int]:
    fut_local_id, timeout, auto_release = json.loads(data)
    if fut_local_id not in state.futures:
        return (
            b"ObjectRef not found! It might be released, see ObjectRef.DisableAutoRelease() doc for more info",
            consts.ErrCode.ObjectRefNotFound,
        )

    obj_ref = state.futures[fut_local_id]
    logger.debug(f"[Py] get obj {obj_ref.hex()}")
    if timeout < 0:
        timeout = None
    try:
        res, code = ray.get(obj_ref, timeout=timeout)
        if auto_release:
            state.futures.release(fut_local_id)
    except ray.exceptions.GetTimeoutError:
        return b"timeout to get object", consts.ErrCode.Timeout
    except ray.exceptions.TaskCancelledError:
        return b"task cancelled", consts.ErrCode.Cancelled
    return res, code


def handle_put_object(data: bytes) -> tuple[bytes, int]:
    fut = ray.put([data, 0])
    # side effect: make future outlive this function (on purpose)
    fut_local_id = state.futures.add(fut)
    return common.uint64_le_packer.pack(fut_local_id), 0


def handle_wait_object(data: bytes) -> tuple[bytes, int]:
    opts = json.loads(data)
    fut_local_ids = opts.pop("object_ref_local_ids")

    futs = []
    fut_hex2idx = {}
    for idx, fut_local_id in enumerate(fut_local_ids):
        if fut_local_id not in state.futures:
            return (
                b"ObjectRef not found! It might be released, see ObjectRef.DisableAutoRelease() doc for more info",
                consts.ErrCode.ObjectRefNotFound,
            )
        fut = state.futures[fut_local_id]
        futs.append(fut)
        fut_hex2idx[fut.hex()] = idx

    ready, not_ready = ray.wait(futs, **opts)

    ready_ids = [fut_hex2idx[i.hex()] for i in ready]
    not_ready_ids = [fut_hex2idx[i.hex()] for i in not_ready]
    ret_data = json.dumps([ready_ids, not_ready_ids]).encode()
    return ret_data, 0


def handle_cancel_object(data: bytes) -> tuple[bytes, int]:
    opts = json.loads(data)
    fut_local_id = opts.pop("object_ref_local_id")
    if fut_local_id not in state.futures:
        return (
            b"ObjectRef not found! It might be released, see ObjectRef.DisableAutoRelease() doc for more info",
            consts.ErrCode.ObjectRefNotFound,
        )
    fut = state.futures[fut_local_id]
    ray.cancel(fut, **opts)
    return b"", 0


def handle_release_object(data: bytes) -> tuple[bytes, int]:
    object_ref_local_id = int.from_bytes(data, "little")
    if object_ref_local_id not in state.futures:
        return b"ObjectRef not found", consts.ErrCode.ObjectRefNotFound
    state.futures.release(object_ref_local_id)
    return b"", 0
