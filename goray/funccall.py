import json

from . import state, utils


def _get_objects(object_pos_to_local_id: dict[int, int]):
    object_positions = []
    object_refs = []
    for pos, local_id in sorted(object_pos_to_local_id.items()):
        object_positions.append(int(pos))
        if local_id not in state.futures:
            return b"object_ref not found!", 1
        object_refs.append(state.futures[local_id])
    return object_positions, object_refs


def decode_funccall_args(data: bytes):
    raw_args, opts_data = utils.unpack_bytes_units(data)
    options: dict = json.loads(opts_data)
    object_pos_to_local_id = options.pop("go_ray_object_pos_to_local_id", {})
    object_positions, object_refs = _get_objects(object_pos_to_local_id)
    return raw_args, options, object_positions, object_refs


def pack_golang_funccall_data(
    raw_args: bytes,
    object_positions: list[int],
    *object_refs: tuple[bytes, int],
) -> tuple[bytes, int]:
    """
    data format: multiple bytes units
    - first unit is raw args data;
    - other units are objectRefs resolved data;
        - resolved data format: | arg_pos:8byte:int64 | data:[]byte |
    """
    data = [raw_args]
    for pos, (raw_res, code) in zip(object_positions, object_refs):
        if code != 0:  # ray task for this object failed
            origin_err_msg = raw_res.decode("utf-8")
            err_msg = (
                f"ray task for the object in {pos}th argument error: {origin_err_msg}"
            )
            return err_msg.encode("utf-8"), code
        data.append(pos.to_bytes(8, byteorder="little") + raw_res)
    return utils.pack_bytes_units(data), 0
