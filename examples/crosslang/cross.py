from goray import x


@x.export
def echo(*args) -> list:
    print(f"python echo receive: {args}")
    return list(args)


@x.export
def hello(name: str):
    return f"hello {name}"


@x.export
def no_return(name: str):
    return


if __name__ == "__main__":
    import os

    here = os.path.dirname(os.path.abspath(__file__))

    lib = x.load_go_lib("out/go.lib")

    lib.func_call("CallPython")

    res = lib.func_call("Echo", 1, "str", b"bytes", [1, 2], {"k": 3})
    print(f"python call go Echo return: {res}")

    res = lib.func_call("Hello", "world")
    print(f"python call go Hello return: {res}")

    res = lib.func_call("NoReturnVal", 1, 2)
    print(f"python call go NoReturnVal return: {res}")

    counter = lib.new_type("Counter", 1)
    print(f"{counter.Incr(10)=}")
