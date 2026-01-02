import os
import time

import pytest
import ray

import goray


def test_pass_py_objref_to_go():
    """Test passing Python ObjectRef to Golang tasks"""
    data_ref = ray.put({"key": "value", "number": 42})

    task = goray.golang_task("Single")
    result_ref = task.remote(data_ref)

    received_data = ray.get(result_ref)
    assert received_data == {"key": "value", "number": 42}
    

def test_local_task():
    pid = goray.golang_local_run_task("Pid")
    assert pid == os.getpid()
    echo = goray.golang_local_run_task(
        "Echo", "str", b"bytes", 123, 1.0, True, [1, "2", []]
    )

    assert echo == ["str", b"bytes", 123, 1.0, True, [1, "2", []]]
    single = goray.golang_local_run_task("Single", "str")
    assert single == "str"
    hello = goray.golang_local_run_task("Hello", "world")
    assert hello == "hello world"
    no_return = goray.golang_local_run_task("NoReturn", "world")
    assert no_return == None

    task = goray.golang_task("ReturnStruct")
    res = task.remote()
    assert ray.get(res) == {
        "Val": 1,
        "Next": {"Val": 2, "Next": {"Val": 3, "Next": None}},
    }


def test_remote_task():
    pid = goray.golang_task("Pid")
    assert ray.get(pid.remote()) != os.getpid()
    echo = goray.golang_task("Echo")
    assert ray.get(echo.remote("str", b"bytes", 123, 1.0, True, [1, "2", []])) == [
        "str",
        b"bytes",
        123,
        1.0,
        True,
        [1, "2", []],
    ]
    single = goray.golang_task("Single")
    assert ray.get(single.remote("str")) == "str"
    hello = goray.golang_task("Hello")
    assert ray.get(hello.remote("world")) == "hello world"
    no_return = goray.golang_task("NoReturn")
    assert ray.get(no_return.remote("world")) == None
    task = goray.golang_task("ReturnStruct")
    res = task.remote()
    assert ray.get(res) == {
        "Val": 1,
        "Next": {"Val": 2, "Next": {"Val": 3, "Next": None}},
    }

    sleep = goray.golang_task("BusySleep", num_cpus=0)
    tasks = [sleep.remote(1) for _ in range(6)]
    start = time.time()
    ray.get(tasks)
    assert time.time() - start < 6, f"task should take at most 6 seconds, but got {time.time() - start}"


def test_local_actor():
    actor = goray.golang_local_new_actor("GoNewCounter", 20)
    assert actor.Incr(10) == 30
    assert actor.Decr(10) == 20
    assert actor.Pid() == os.getpid()
    assert actor.Echo("str", b"bytes", 123, 1.0, True, [1, "2", []]) == [
        "str",
        b"bytes",
        123,
        1.0,
        True,
        [1, "2", []],
    ]
    assert actor.Single("str") == "str"
    assert actor.Hello("world") == "hello world"
    assert actor.NoReturn("world") == None


def test_remote_actor():
    actor = goray.golang_actor_class("GoNewCounter", num_cpus=0).remote(20)
    assert ray.get(actor.Incr.remote(10)) == 30
    assert ray.get(actor.Decr.remote(10)) == 20
    assert ray.get(actor.Pid.remote()) != os.getpid()
    assert ray.get(
        actor.Echo.remote("str", b"bytes", 123, 1.0, True, [1, "2", []])
    ) == ["str", b"bytes", 123, 1.0, True, [1, "2", []]]
    assert ray.get(actor.Single.remote("str")) == "str"
    assert ray.get(actor.Hello.remote("world")) == "hello world"
    assert ray.get(actor.NoReturn.remote("world")) == None


def test_task_with_options():
    """Test golang tasks with ray options"""
    # Test task with cpu option
    task = goray.golang_task("BusySleep", num_cpus=0.5)
    start = time.time()
    result = task.remote(1)  # 1 second sleep
    ray.get(result)
    elapsed = time.time() - start
    assert elapsed >= 0.9  # Should take at least 0.9 seconds
    
    # Test task with memory option
    task = goray.golang_task("Echo", memory=50*1024*1024)  # 50MB
    result = task.remote("memory_test", 123)
    assert ray.get(result) == ["memory_test", 123]


def test_objectref_passing():
    """Test passing ObjectRef between tasks"""
    # Create an ObjectRef
    data_ref = ray.put({"key": "value", "number": 42})
    
    # Pass ObjectRef to golang task
    task = goray.golang_task("Single")
    result = task.remote(data_ref)
    
    # The golang task should receive the actual data
    received_data = ray.get(result)
    assert received_data == {"key": "value", "number": 42}


def test_complex_data_types():
    """Test complex data type conversion between Python and Go"""
    # Test nested structures
    complex_data = {
        "string": "hello",
        "integer": 42,
        "float": 3.14,
        "boolean": True,
        "bytes": b"binary_data",
        "list": [1, 2, 3],
        "nested_dict": {
            "inner_string": "world",
            "inner_list": [4, 5, 6]
        },
        "list_of_dicts": [
            {"a": 1, "b": 2},
            {"c": 3, "d": 4}
        ],
        "none_value": None
    }
    
    task = goray.golang_task("Echo")
    result = task.remote(complex_data)
    received = ray.get(result)
    
    # Should receive back as a list (since Go Echo returns []any)
    assert len(received) == 1
    assert received[0] == complex_data


def test_multiple_return_values():
    """Test golang tasks with multiple return values"""
    task = goray.golang_task("ReturnStruct")
    result = task.remote()
    
    # Go tasks with multiple returns are returned as list in Python
    struct_data = ray.get(result)
    expected = {
        "Val": 1,
        "Next": {"Val": 2, "Next": {"Val": 3, "Next": None}},
    }
    assert struct_data == expected


def test_actor_lifecycle():
    """Test actor creation, method calls, and lifecycle"""
    # Create actor with initial value
    actor = goray.golang_actor_class("GoNewCounter").remote(100)
    
    # Test method chaining
    result1 = actor.Incr.remote(50)  # 150
    result2 = actor.Incr.remote(result1)  # 150 + 150 = 300
    
    final_value = ray.get(result2)
    assert final_value == 300
    
    # Test different methods
    value_after_decr = ray.get(actor.Decr.remote(50))  # 300 - 50 = 250
    assert value_after_decr == 250


def test_actor_with_options():
    """Test actor creation with ray options"""
    # Create actor with specific resource requirements
    actor = goray.golang_actor_class("GoNewCounter", num_cpus=1, memory=100*1024*1024).remote(0)
    
    # Test the actor works
    result = ray.get(actor.Incr.remote(10))
    assert result == 10


def test_named_actor():
    """Test named actor creation and retrieval"""
    # Create named actor
    actor_name = "test_named_counter"
    actor = goray.golang_actor_class("GoNewCounter").options(name=actor_name).remote(1000)
    
    # Use the actor
    result = ray.get(actor.Incr.remote(100))
    assert result == 1100
    
    # Get the same actor by name
    retrieved_actor = ray.get_actor(actor_name)
    result2 = ray.get(retrieved_actor.Decr.remote(200))
    assert result2 == 900  # 1100 - 200


def test_concurrent_tasks():
    """Test concurrent execution of golang tasks"""
    task = goray.golang_task("BusySleep", num_cpus=0)
    
    # Launch multiple tasks concurrently
    futures = []
    start_time = time.time()
    
    for i in range(5):
        future = task.remote(1)  # Each task sleeps 1 second
        futures.append(future)
    
    # Wait for all tasks to complete
    ray.get(futures)
    elapsed = time.time() - start_time
    
    # Should take around 1 second (concurrent), not 5 seconds
    assert elapsed < 3.0  # Allow some overhead


def test_actor_state_isolation():
    """Test that different actor instances have isolated state"""
    # Create two separate actor instances
    actor1 = goray.golang_actor_class("GoNewCounter").remote(10)
    actor2 = goray.golang_actor_class("GoNewCounter").remote(20)
    
    # Modify each actor independently
    ray.get(actor1.Incr.remote(5))  # actor1: 15
    ray.get(actor2.Incr.remote(10))  # actor2: 30
    
    # Verify they have different states
    value1 = ray.get(actor1.Incr.remote(0))  # Just get current value
    value2 = ray.get(actor2.Incr.remote(0))  # Just get current value
    
    assert value1 == 15
    assert value2 == 30


def test_error_handling():
    """Test error handling in cross-language calls"""
    # Test with non-existent task
    with pytest.raises(Exception):
        task = goray.golang_task("NonExistentTask")
        ray.get(task.remote())
    
    # Test with non-existent actor
    with pytest.raises(Exception):
        actor = goray.golang_actor_class("NonExistentActor").remote()


def test_large_data_transfer():
    """Test transferring large data between Python and Go"""
    # Create large data
    large_list = list(range(10000))
    
    task = goray.golang_task("Echo")
    result = task.remote(large_list)
    
    received = ray.get(result)
    assert len(received) == 1
    assert received[0] == large_list


def test_binary_data():
    """Test binary data transfer"""
    binary_data = b"This is binary data with special chars: \x00\x01\x02\xff"
    
    task = goray.golang_task("Echo")
    result = task.remote(binary_data)
    
    received = ray.get(result)
    assert len(received) == 1
    assert received[0] == binary_data


def test_mixed_data_types():
    """Test mixed data types in single call"""
    mixed_args = [
        "string",
        42,
        3.14,
        True,
        [1, 2, 3],
        {"key": "value"},
        b"binary",
    ]
    
    task = goray.golang_task("Echo")
    result = task.remote(*mixed_args)
    
    received = ray.get(result)
    assert received == mixed_args


def test_golang_struct_conversion():
    """Test golang struct to python dict conversion"""
    task = goray.golang_task("ReturnStruct")
    result = task.remote()
    
    struct_data = ray.get(result)
    
    # Verify structure
    assert struct_data["Val"] == 1
    assert struct_data["Next"]["Val"] == 2
    assert struct_data["Next"]["Next"]["Val"] == 3
    assert struct_data["Next"]["Next"]["Next"] is None


def test_actor_method_chaining():
    """Test chaining actor method calls with ObjectRefs"""
    actor = goray.golang_actor_class("GoNewCounter").remote(0)
    
    # Chain multiple operations
    ref1 = actor.Incr.remote(10)  # 10
    ref2 = actor.Incr.remote(ref1)  # 10 + 10 = 20
    ref3 = actor.Decr.remote(5)   # 20 - 5 = 15
    
    results = ray.get([ref1, ref2, ref3])
    assert results == [10, 20, 15]


def test_timeout_behavior():
    """Test timeout behavior with golang tasks"""
    task = goray.golang_task("BusySleep")
    future = task.remote(2)  # 2 seconds sleep
    start = time.time()
    # Test short timeout
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(future, timeout=0.5)  # Should timeout
    # Test sufficient timeout
    ray.get(future, timeout=5.0)  # Should succeed
    elapsed = time.time() - start
    assert elapsed >= 1.8  # Should have taken around 2 seconds


def test_no_return_task():
    """Test golang task with no return value"""
    task = goray.golang_task("NoReturn")
    result = task.remote("test")
    
    # Should return None
    assert ray.get(result) is None


def test_actor_no_return_method():
    """Test actor method with no return value"""
    actor = goray.golang_actor_class("GoNewCounter").remote(0)
    result = actor.NoReturn.remote("test")
    
    # Should return None
    assert ray.get(result) is None

def test_panic_task():
    task = goray.golang_task("Panic")
    result = task.remote("test")

    with pytest.raises(RuntimeError) as excinfo:
        ray.get(result)
    assert "golang panic" in str(excinfo.value)

@goray.remote
def start_python_tests():
    # return pytest.main(["-s", __file__])  this will block in some
    test_cases = [
        func
        for name, func in globals().items()
        if name.startswith("test_") and callable(func)
    ]
    print(f"{len(test_cases)} Python test cases found.")
    for test in test_cases:
        print(f"Running {test.__name__}...")
        test()
    print("All Python test cases finished.")
    return 0
