import json

from . import  utils

def encode_golang_funccall_arguments(
    name: str,
    encoded_args: bytes,
    object_positions: list[int],
    *object_refs: tuple[bytes, int],
) -> tuple[bytes, int]:
    """
    encode the arguments for golang function call (or method call)

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


def decode_funccall_arguments(data: bytes):
    """
    decode the arguments for python function call from golang

    data format: multiple bytes units
    - first unit is encoded args data;
    - second unit is json encoded args data, which contains function/method name;
    """
    raw_args, opts_data = utils.unpack_bytes_units(data)
    options: dict = json.loads(opts_data)
    return raw_args, options