from . import api

@api.remote(num_cpus=100)
def test(*args) -> list:
    print(f"python task receive: {args}")
    return list(args)
