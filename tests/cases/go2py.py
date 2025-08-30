import threading
import time
import os
import struct

import goray


@goray.remote(num_cpus=1000)
def overwrite_ray_options():
    pass


@goray.remote
def echo(*args) -> tuple:
    return args


@goray.remote
def single(arg):
    return arg


@goray.remote
def hello(name: str):
    return f"hello {name}"


@goray.remote
def get_pid() -> int:
    return os.getpid()


@goray.remote
def no_return(name: str):
    return


@goray.remote
def no_args():
    return 42


@goray.remote
def busy(sec: int) -> int:
    time.sleep(sec)
    return threading.current_thread().ident


@goray.remote
def multiple_returns(a: int, b: str):
    """Return multiple values to test Get2, Get3, etc."""
    return a, b, a * 2, b.upper(), True


@goray.remote
def three_returns(x: int):
    """Return exactly 3 values to test Get3"""
    return x, x * 2, x * 3


@goray.remote
def four_returns(x: int):
    """Return exactly 4 values to test Get4"""
    return x, x * 2, x * 3, x * 4


@goray.remote
def five_returns(x: int):
    """Return exactly 5 values to test Get5"""
    return x, x * 2, x * 3, x * 4, x * 5


@goray.remote
def return_none():
    """Return None to test None handling"""
    return None


@goray.remote
def return_empty_values():
    """Return various empty values"""
    return [], {}, "", 0, False


@goray.remote
def complex_types(data):
    """Test complex nested data structures"""
    return data


@goray.remote
def type_conversion_test():
    """Test various Python types for golang conversion"""
    return {
        "int": 42,
        "float": 3.14,
        "bool": True,
        "str": "hello",
        "bytes": b"world",
        "list": [1, 2, 3],
        "dict": {"a": 1, "b": 2},
        "none": None,
        "nested": {
            "list_of_dicts": [{"x": 1}, {"y": 2}],
            "dict_of_lists": {"nums": [1, 2, 3], "strs": ["a", "b"]},
        }
    }


@goray.remote
def raise_exception():
    """Test exception handling"""
    raise ValueError("Python exception for testing")


@goray.remote 
def return_very_large_int():
    """Test large integer handling"""
    return 2**63 - 1  # Max int64


@goray.remote
def return_special_floats():
    """Test special float values"""
    import math
    return {
        "inf": float('inf'),
        "neg_inf": float('-inf'), 
        "nan": float('nan'),
        "zero": 0.0,
        "neg_zero": -0.0
    }


@goray.remote
def map_with_struct_values():
    """Test map[string]struct{} equivalent in Python"""
    return {"key1": {}, "key2": {}, "key3": {}}


@goray.remote
def unicode_strings():
    """Test Unicode string handling"""
    return {
        "chinese": "你好世界",
        "emoji": "🚀🌟💻", 
        "special": "tab:\t, newline:\n, quote:\"",
        "empty": ""
    }


@goray.remote
def array_operations(arr):
    """Test various array operations"""
    return {
        "original": arr,
        "length": len(arr),
        "doubled": [x * 2 for x in arr],
        "filtered": [x for x in arr if x > 5],
        "sum": sum(arr) if arr else 0
    }


@goray.remote  
def mixed_type_list():
    """Return a list with mixed types"""
    return [1, "string", 3.14, True, None, [1, 2], {"key": "value"}]


@goray.remote
def nested_collections():
    """Test deeply nested collections"""
    return {
        "matrix": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        "list_of_maps": [{"a": 1}, {"b": 2}, {"c": 3}],
        "map_of_lists": {
            "numbers": [1, 2, 3, 4, 5], 
            "letters": ["a", "b", "c"],
            "mixed": [1, "a", 2.5, True]
        }
    }


@goray.remote
def timeout_task(sleep_time: float):
    """Test timeout functionality"""
    time.sleep(sleep_time)
    return "completed"


@goray.remote
def large_data_task():
    """Test large data transfer"""
    return list(range(10000))


@goray.remote
def pack_uint64(n) -> bytes:
    """Pack a uint64 number into bytes"""
    return struct.pack("<Q", n)


@goray.remote
class PyActor:
    def __init__(self, *args):
        self.args = args
        self.state = {}

    def get_args(self):
        return self.args

    def echo(self, *args):
        return args

    def hello(self, name: str):
        return f"hello {name}"

    def single(self, arg):
        return arg

    def busy(self, sec: int) -> int:
        time.sleep(sec)
        return sec

    def no_return(self, name: str):
        return

    def no_args(self):
        return 42

    def set_state(self, key: str, value):
        """Test stateful actor operations"""
        self.state[key] = value
        return f"set {key} = {value}"

    def get_state(self, key: str):
        """Get state value"""
        return self.state.get(key)

    def get_all_state(self):
        """Get all state"""
        return self.state.copy()

    def multiply_returns(self, a: int, b: str):
        """Return multiple values from actor method"""
        return a, b, a * 2, b.upper()

    def raise_error(self):
        """Test actor method exception"""
        raise RuntimeError("Actor method error")

    def increment_counter(self, step: int = 1):
        """Test actor with default parameters"""
        if not hasattr(self, 'counter'):
            self.counter = 0
        self.counter += step
        return self.counter

    def reset_counter(self):
        """Reset the counter"""
        self.counter = 0
        return self.counter

    def batch_operations(self, operations: list):
        """Test batch operations with actor state"""
        results = []
        for op in operations:
            if op['action'] == 'set':
                self.state[op['key']] = op['value']
                results.append(f"set {op['key']}")
            elif op['action'] == 'get':
                results.append(self.state.get(op['key'], 'not_found'))
            elif op['action'] == 'delete':
                if op['key'] in self.state:
                    del self.state[op['key']]
                    results.append(f"deleted {op['key']}")
                else:
                    results.append(f"{op['key']} not found")
        return results
