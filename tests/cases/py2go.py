import pytest
import ray

import goray


def test_ReturnStruct():
    task = goray.golang_task("ReturnStruct")
    res = task.remote()
    assert ray.get(res) == {
        "Val": 1,
        "Next": {"Val": 2, "Next": {"Val": 3, "Next": None}},
    }


@goray.remote
def start_python_tests():
    pytest.main(["-s", __file__])


if __name__ == "__main__":
    pytest.main([__file__])
