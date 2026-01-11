# GoRay Cross-Language Programming

GoRay provides seamless cross-language integration between Go and Python in Ray applications.
Specifically, it supports the following features:

- Cross-language remote call of Ray tasks and actors
    - Call Python Ray tasks from Go; call Go Ray tasks from Python
    - Create and call Python actors from Go; create and call Go actors from Python
    - Pass Go task `ObjectRef` to Python task, and vice versa
    - Create a named actor in Go, and get its handle in Python, and vice versa *
- Local cross-language call (in-process)
    - Python call Go functions, create Go type instances and invoke methods
    - Go call Python functions, create Python class instances and invoke methods

> *Currently, when you create a Python named actors in Python, it's not supported to get its handle in Go.
> This will be supported in future releases.

## Overview

In Python:

- Use `goray.remote` to declare Ray tasks/actors (replaces `ray.remote`).
- Use `goray.golang_actor_class(name: str)` to get a Go Ray actor class and create actors as usual.
- Use `goray.golang_task(name: str)` to get a Go Ray task and invoke it like a Python Ray task.
- Use `goray.golang_local_run_task(name: str, *args)` to call a Go function in-process.
- Use `goray.golang_local_new_actor(name: str, *args)` to instantiate a Go class in-process.

In Go:

- `ray.RemoteCallPyTask(name, args...) -> ObjectRef` — Submit a Python Ray task.
- `ray.NewPyActor(name, args...) -> ActorHandle` — Create a Python Ray actor.
- `ray.LocalCallPyTask(name, args...) -> LocalPyCallResult` — Call a Python function in-process.
- `ray.NewPyLocalInstance(name, args...) -> PyLocalInstance` — Create a Python class instance in-process.

## User Guide

### Go remote call Python tasks and actors

Define Python Ray tasks and actors in a Python file:

```python
# app.py
import goray

# Define Python Ray task
@goray.remote
def square(x):
    return x * x

# Define Python Ray actor
@goray.remote
class Storage:
    def __init__(self):
        self.kv = {}

    def put(self, key, value):
        self.kv[key] = value

    def get(self, key):
        return self.kv.get(key)
```

Call Python tasks and actors from Go:

```go
// main.go
package main

import (
	"fmt"
	"github.com/ray4go/go-ray/ray"
)

func init() {
	ray.Init(nil, nil, func() int {
        // Call Python Ray task
		obj := ray.RemoteCallPyTask("square", 8)
		res, err := ray.Get1[int](obj)
		fmt.Printf("RemoteCallPyTask square, res:%d, err: %v\n", res, err)

        // Create and call Python Ray actor
		actorHandle := ray.NewPyActor("Storage")
		actorHandle.RemoteCall("put", "key1", "value1")
		obj2 := actorHandle.RemoteCall("get", "key1")
		res2, err2 := ray.Get1[string](obj2)
		fmt.Printf("NewPyActor Storage get, res:%s, err: %v\n", res2, err2)

		return 0
	})
}

func main() {}
```

Run the Ray application:

```bash
go build -buildmode=c-shared -o ./build/rayapp .

# Ray standalone environment
goray --py-defs app.py  ./build/rayapp

# Ray cluster
export RAY_ADDRESS="http://RAY_CLUSTER_ADDRESS"  # Replace with your cluster address
ray job submit --working-dir=./ -- goray --py-defs app.py ./build/rayapp
```

When call python task and actor methods, the return value is always a single value (if any).
Use `ray.Get1[T](objectRef)` or `objectRef.GetInto(&val)` to get result.

### Python remote call Go tasks and actors

Define Go Ray tasks and actors in a Go file:

```go
// app.go
package main

import (
	"fmt"
	"github.com/ray4go/go-ray/ray"
)

type Tasks struct{}

func (Tasks) Hello(name string) string {
	return fmt.Sprintf("Hello %s", name)
}

type Actors struct{}

type Counter struct {
	num int
}

func (Actors) Counter(n int) *Counter {
	return &Counter{num: n}
}

func (actor *Counter) Incr(n int) int {
	actor.num += n
	return actor.num
}

func init() {
	ray.Init(Tasks{}, Actors{}, func() int {
		ray.LocalCallPyTask("main")  // Call Python main function
		return 0
	})
}

func main() {}
```

Call Go tasks and actors from Python:

```python
# app.py
import ray
import goray

# Define Python local function
@goray.local
def main():
    # Call Go task from Python
    hello = goray.golang_task("Hello")
    result = hello.remote("World")
    print("Call Hello task:", ray.get(result))

    # Call Go actor from Python
    Counter = goray.golang_actor_class("Counter")
    counter = Counter.remote(0)
    result = counter.Incr.remote(1)
    print("Call Counter.Incr method:", ray.get(result))
```

Run the Ray application in same way as above.

### Customizing application entrypoint

In previous examples, we use `goray` CLI tool to start the GoRay application. You can also customize your own application entrypoint. This is useful when you want to customize the `ray.init()` or do some initialization before starting GoRay.

`goray` provides a `goray.start()` function to initialize GoRay & Ray environment and start the Go driver function. That's how `goray` CLI tool works internally (see [`goray/cli.py`](../goray/cli.py)). You can call `goray.start()` in your own entrypoint script as follows:

```python
# entrypoint.py
import goray

ray_init_args = dict(address="auto")
ret = goray.start("path/to/go_ray_app_binary", py_defs_path="path/to/py_defs_file", **ray_init_args)
print(f"Go driver function exit code: {ret}")
```

After `goray.start()` returns, you can continue to call your Python/Go Ray tasks and actors as usual.

Now you can run your custom entrypoint to start the GoRay application:

```bash
# Ray standalone environment
python entrypoint.py

# Ray cluster
export RAY_ADDRESS="http://RAY_CLUSTER_ADDRESS"  # Replace with your cluster address
ray job submit --working-dir=./ -- python entrypoint.py
```

## Cross-language data serialization

Type mapping between Python and Go uses msgpack. Supported types: integers, floats, booleans, strings, binary data,
lists (Go slices), maps, and nil. Map keys must be strings or integers. The Go side additionally supports structs and
pointers. See the [Cross-Language Type Conversion Guide](crosslang_types.md).


## Notes

- When Python calls Go, if the Go task/method has a single return value, Python receives a single value. For multiple return values, Python receives a list.
- Go calls Python: Return values are always delivered to Go as a single value (if any). Use `ray.Get1[T](objectRef)` or `objectRef.GetInto(&val)` to read it.