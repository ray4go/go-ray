# GoRay

Go bindings for Ray Core.

[API Documentation](https://pkg.go.dev/github.com/ray4go/go-ray/ray)

## Features

- **Pure Go** — Build Ray applications in Go without touching Python
- **Seamless Polyglot** — Hybrid Python–Go development with cross-language Task/Actor invocation
- **Strong Compatibility** — Clean implementation with zero hacks
- **Type-safe Remote Calls** — Compile-time safety and full IDE support via code generation


## Quick Start

Init a go project:

```bash
mkdir goray-app && cd goray-app
go mod init rayapp
```

Add a main.go file:

```golang
package main

import (
	"fmt"
	"github.com/ray4go/go-ray/ray"
)

type Tasks struct{}

func (Tasks) Square(x float64) float64 {
	return x * x
}

func driver() int {
	obj := ray.RemoteCall("Square", 2.0)
	res, err := ray.Get1[float64](obj)
	fmt.Printf("Square(2.0) = %f, err: %v\n", res, err)
	return 0
}

func init() {
	ray.Init(Tasks{}, nil, driver)
}

func main() {}
```

Build and run the application:

```bash
go mod tidy
go build -buildmode=c-shared -o rayapp .

pip install "ray[default]" goray
goray rayapp
```

## Usage

### Install

```bash
go get github.com/ray4go/go-ray/ray
```

Requires Go 1.21+.

### Example GoRay Application

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/ray4go/go-ray/ray"
)

// ray tasks register
type Tasks struct{}

func (_ Tasks) TheAnswerOfWorld() int64 {
	time.Sleep(1 * time.Second)
	return 42
}

func (_ Tasks) Divide(a, b int64) (int64, int64) {
	return a / b, a % b
}

// ray actors register
type Actors struct{}

func (_ Actors) Counter(n int) *Counter {
	return &Counter{num: n}
}

// ray actor
type Counter struct {
	num int
}

func (c *Counter) Incr(n int) int {
	fmt.Printf("Incr %d -> %d\n", c.num, c.num+n)
	c.num += n
	return c.num
}

func (c *Counter) Decr(n int) int {
	fmt.Printf("Decr %d -> %d\n", c.num, c.num-n)
	c.num -= n
	return c.num
}

func init() {
	// Initialize and register Ray Tasks, Actors, and the driver
	ray.Init(Tasks{}, Actors{}, driver)
}

func driver() int {
	// ray task remote call
	answerObjRef := ray.RemoteCall("TheAnswerOfWorld")
	objRef := ray.RemoteCall("Divide", answerObjRef, 5, ray.Option("num_cpus", 2), ray.Option("memory", 100*1024*1024))
	res, remainder, err := ray.Get2[int64, int64](objRef)
	if err != nil {
		log.Panicf("remote task error: %v", err)
	}
	fmt.Printf("call Divide -> %#v, %#v \n", res, remainder)

	// ray actor remote call
	cnt := ray.NewActor("Counter", 1)
	obj := cnt.RemoteCall("Incr", 1)
	var res2 int
	err2 := obj.GetInto(&res2)
	fmt.Println("Incr ", res2, err2)

	obj2 := cnt.RemoteCall("Incr", 1)
	obj3 := cnt.RemoteCall("Incr", obj2)
	fmt.Println(obj3.GetAll())
	return 0
}

// main function won't be called but cannot be omitted (it's required only for compilation)
func main() {}
```

### Task and Actor Registration

Use `ray.Init(taskRegister, actorRegister, driver, ...rayOptions)` to register Ray tasks, actors, and the driver and init the Ray environment:

- All exported methods on the `taskRegister` are registered as Ray tasks.
- Exported methods on the `actorRegister` are used to create actors; each method serves as the actor’s constructor.
- The driver function must have signature `func() int`. It is called when the ray application starts and should return
  an integer as exit code.
- `rayOptions` are used to provide the [`ray.init()`](https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html#ray.init) options. In most cases, it's enough to pass no options.

`ray.Init()` should be called in the main package's `init()` function. All other GoRay APIs must be called within the
driver function and its spawned remote actors/tasks.

The main function of the main package won't be called but cannot be omitted.

### Remote Invocation

Tasks:

- Use `ray.RemoteCall(taskName, args...) -> ObjectRef` to invoke a Ray task asynchronously.
- Configure tasks by appending zero or more `ray.Option(key, value)` arguments (equivalent to Ray’s [
  `.options(...)`](https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote_function.RemoteFunction.options.html#ray.remote_function.RemoteFunction.options)).
- Parameters and return values may be composite types or structs (struct fields must be exported). Variadic arguments
  and multiple return values are supported.

Actors:

- Use `ray.NewActor(actorTypeName, args...) -> ActorHandle` to create a remote actor. Ray options are also supported via
  `ray.Option`.
- `actorTypeName` matches the constructor method name of `actors` registered via `ray.Init(tasks, actors, driver)`.
  Constructor arguments (excluding options) are passed to that method.
- Use `actorHandle.RemoteCall(methodName, args...) -> ObjectRef` to invoke actor methods. Semantics match
  `ray.RemoteCall()`.

### Retrieving Return Values

Remote calls return an `ObjectRef`, which represents a future. Because Go supports multiple return values, `ObjectRef`
does too. Retrieve results in any of these ways:

- Pass pointers with `ObjectRef.GetInto(ptrs...) -> error`.

- Use typed helpers:
    - For no returns remote call: `ObjectRef.Get0() -> error`
    - One return: `ray.Get1[T]() -> (T, error)`
    - Two returns: `ray.Get2[T1, T2]() -> (T1, T2, error)`
    - And so on.

- Use `ObjectRef.GetAll() -> []any` and assert to concrete types.

You may pass `ObjectRef` as arguments to `RemoteCall()`, but only when it has a single return value and the result type
is compatible with the parameter type.

Passing an `ActorHandle` as a parameter to `RemoteCall()` is not supported yet. A workaround is to create a named actor
(via `NewActor(typeName, ray.Option("name", actorName))`) and pass the actor's name to `RemoteCall()`.
In the remote task/actor, use `ray.GetActor(actorName)` to get the handle.

### Parameter and Return Types

Arguments and return values are serialized via [msgpack](https://msgpack.org/):

- Supported: integers, floats, booleans, strings, binary data, slices, maps, nil, structs, and their pointer types
- Map keys must be strings or integers

### Other APIs

- `Put(data any) -> (SharedObject, error)` — Store an object in Ray’s object store. The returned SharedObject can be
  passed to other tasks/actors (just like `ObjectRef`).
- `Wait(objRefs []ObjectRef, requestNum int, opts ...*option) -> (ready []ObjectRef, pending []ObjectRef, error)` — Wait
  until the specified number of `ObjectRef`s complete.
- `ObjectRef.Cancel(opts ...*option) -> error` — Cancel a remote task (task or actor method).
- `GetActor(name string, opts ...*option) -> (*ActorHandle, error)` — Get a handle to a named actor instance.
- `actorHandle.Kill(opts ...*option)` — Terminate an actor instance.

Where `*option` represents Ray API parameters, created with `ray.Option(name string, val any)`. For supported
parameters, see the [Ray official API docs](https://docs.ray.io/en/latest/ray-core/api/core.html).

### Compilation

```bash
go build -buildmode=c-shared -o ./build/rayapp .
```

You must compile with `-buildmode=c-shared` to produce a shared library.

### Submitting Ray Jobs

#### Ray standalone environment

1) Install the official Ray Python SDK:

```bash
pip install "ray[default]"
```

2) Install the go-ray Python driver:

```bash
pip install goray
```

3) Run the Ray job:

```bash
goray ./build/rayapp
```

#### Ray cluster

Prerequisite: Install the go-ray Python driver on the cluster.

Submit a job:

```bash
export RAY_ADDRESS="http://RAY_CLUSTER_ADDRESS"  # Replace with your cluster address
ray job submit --working-dir=./ -- goray --cluster ./build/rayapp
```

Notes:

- The compiled binary must be inside the working directory (or a subdirectory) and not excluded by .gitignore.
- The binary path passed to `goray` must be relative to `--working-dir`.

<details>
  <summary>Command details</summary>

To run Go in Ray, the compiled binary must be distributed to all nodes. `--working-dir=./` packages and ships files from
the working directory to each node.

`goray` is a Python entry point that bridges Python and Go. The binary path passed to `goray` is resolved on the
cluster. Because the job’s working directory is not known in advance, a relative path is required. If you distribute the
binary by other means, you may use an absolute path.
</details>

### Cross-language Invocation

Cross-language features:

- Cross-language remote call of Ray tasks and actors
    - Call Python Ray tasks from Go; call Go Ray tasks from Python
    - Create and call Python actors from Go; create and call Go actors from Python
    - Pass Go `ObjectRef`s to Python, and vice versa
    - Create a named actor in Go, and get its handle in Python, and vice versa
- Local cross-language calls (in-process)
    - Go calls Python functions in-process
    - Python calls Go functions in-process
    - Python instantiates Go classes and calls their methods in-process

More details and examples:

- [GoRay Cross-Language Programming Documentation](docs/crosslang.md)
- [Example](examples/crosslang/)


### Type-safe Wrappers

APIs such as `ray.RemoteCall()`, `ray.NewActor()`, and `actorHandle.RemoteCall()` use string for names and `any` for
arguments, and require manual casting of results. This results in poor IDE support and a suboptimal developer
experience.

GoRay provides a `goraygen` CLI to generate type-safe wrappers for tasks and actors.

Workflow:

1) Annotate the  struct with `// raytasks` and the actor register struct with `// rayactors`.
2) Generate wrappers:

```bash
go install github.com/ray4go/goraygen
goraygen /path/to/your/package/
```

`goraygen` generates a `ray_workloads_wrapper.go` file in the package directory, containing wrappers for all tasks and
actors.

See: [goraygen Documentation](https://github.com/ray4go/goraygen)

## Best Practices

Parameter and return types:

- Parameter and return types can be primitive types, composites, structs, and their pointers.
- You should not use interface types as return values except for `any` type.

Error handling:

- Do not return `error` (it is an interface). Prefer numeric or string error codes/messages.

## How It Works

Go can compile to shared libraries and expose C APIs via cgo. Python can call C libraries via ctypes. This enables
Python to call into Go. With Go reflection, Python can dynamically invoke Go functions/methods.

By passing ctypes-based C callback functions from Python into Go, Go can call back into Python.

msgpack is used to serialize arguments and return values, enabling cross-language data exchange between Python and Go.
