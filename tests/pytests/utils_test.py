from goray.utils import parse_function_name

def test_parse_function_name():
    code = """
def func1():
    pass

async def func2():
    pass

class MyClass:
    def method(self):
        pass

def func3():
    def nested():
        pass
    pass
"""
    expected = ["func1", "func3"]
    result = parse_function_name(code)
    assert result == expected
