import json

from . import state, utils


def _get_objects(arg_pos_to_object: dict[int, dict]):
    object_positions = []
    object_refs = []
    release_obj_ids = set()
    for pos, obj in sorted(arg_pos_to_object.items()):
        object_positions.append(int(pos))
        local_id, auto_release = int(obj["id"]), obj["auto_release"]
        if local_id not in state.futures:
            raise RuntimeError(
                "ObjectRef not found! It might be released, see ObjectRef.DisableAutoRelease() doc for more info"
            )
        object_refs.append(state.futures[local_id])
        if auto_release:
            release_obj_ids.add(local_id)

    # todo: release object refs after the remote call
    for local_id in release_obj_ids:
        state.futures.release(local_id)
    return object_positions, object_refs


def decode_funccall_args(data: bytes):
    raw_args, opts_data = utils.unpack_bytes_units(data)
    options: dict = json.loads(opts_data)
    arg_pos_to_object = options.pop("go_ray_arg_pos_to_object", {})
    object_positions, object_refs = _get_objects(arg_pos_to_object)
    return raw_args, options, object_positions, object_refs


def pack_golang_funccall_data(
    name: str,
    encoded_args: bytes,
    object_positions: list[int],
    *object_refs: tuple[bytes, int],
) -> tuple[bytes, int]:
    """
    data format: multiple bytes units
    - first unit is function/actor/method name
    - second unit is encoded args data;
    - other units are objectRefs resolved data;
        - resolved data format: | arg_pos:8byte:int64 | data:[]byte |
    """
    data = [name.encode("utf8"), encoded_args]
    for pos, (raw_res, code) in zip(object_positions, object_refs):
        if code != 0:  # ray task for this object failed
            origin_err_msg = raw_res.decode("utf-8")
            err_msg = (
                f"ray task for the object in {pos}th argument error: {origin_err_msg}"
            )
            return err_msg.encode("utf-8"), code
        data.append(pos.to_bytes(8, byteorder="little") + raw_res)
    return utils.pack_bytes_units(data), 0
