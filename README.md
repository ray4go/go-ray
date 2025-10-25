# GoRay

Ray Core for Golang

## Features

- **Pure Golang** - Write pure Golang code in your ray application without touching Python
- **Seamless Polyglot** - Support for hybrid Python-Go development with seamless cross-language Task/Actor invocation
- **Zero Hacks** - Clean implementation with strong compatibility
- **Type-Safe Remote Calls** - Compile-time safety and full IDE support via codegen


## Usage

### Installing GoRay SDK

```bash
go get github.com/ray4go/go-ray/ray
```

Requires Golang 1.21 or higher.

### Hello World

```go
package main

import (
  "fmt"
  "log"
  "time"

  "github.com/ray4go/go-ray/ray"
)

func init() {
  // Initialize and register ray tasks, actors, and ray driver
  ray.Init(tasks{}, actors{}, driver)
}

func driver() int {
  // ray task
  answerObjRef := ray.RemoteCall("TheAnswerOfWorld")
  objRef := ray.RemoteCall("Divide", answerObjRef, 5, ray.Option("num_cpus", 2), ray.Option("num_cpus", 2))
  res, remainder, err := ray.Get2[int64, int64](objRef)
  if err != nil {
    log.Panicf("remote task error: %v", err)
  }
  fmt.Printf("call Divide -> %#v, %#v \n", res, remainder)

  // ray actor
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

// raytasks
type tasks struct{}
type actors struct{}

func (_ tasks) TheAnswerOfWorld() int64 {
  time.Sleep(1 * time.Second)
  return 42
}

func (_ tasks) Divide(a, b int64) (int64, int64) {
  return a / b, a % b
}

// ray actor
type counter struct {
  num int
}

func (_ actors) Counter(n int) *counter {
  return &counter{num: n}
}

func (c *counter) Incr(n int) int {
  fmt.Printf("Incr %d -> %d\n", c.num, c.num+n)
  c.num += n
  return c.num
}

func (c *counter) Decr(n int) int {
  fmt.Printf("Decr %d -> %d\n", c.num, c.num-n)
  c.num -= n
  return c.num
}

// main function won't be called but cannot be omitted
func main() {}
```

**Task Registration**

Use `ray.Init(tasks, actors, driver)` to register ray tasks, actors, and driver. Where:
- All public methods of the tasks variable will be registered as ray tasks
- Public methods of the actors variable are used to create actors; the method serves as the actor's constructor, and the method name becomes the actor type name. The constructor supports parameters, and its return value should be the corresponding actor instance
- The driver function has the signature `func() int` and will be executed as the ray driver, with its return value representing the exit code of the driver process

`ray.Init()` should be called in the `init()` function of the main package. All other GoRay APIs can only be called within the driver function and its spawned remote tasks.

The main function of the main package won't be called but cannot be omitted.

**Remote Invocation**

Use `ray.RemoteCall(taskName, args...) -> ObjectRef` to asynchronously invoke a ray task.
Configure the task by passing zero or more `ray.Option(key, value)` at the end of the `RemoteCall()` parameter list (corresponding to [`.options(...)`](https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote_function.RemoteFunction.options.html#ray.remote_function.RemoteFunction.options) in Ray).
Task parameters and return values can be composite types or structs (struct fields must be public), supporting variadic arguments and multiple return values.

Use `ray.NewActor(actorTypeName, args...)` to create a remote actor instance, with the parameter list also supporting `ray.Option(key, value)`.
`ray.NewActor` returns an actor handle. Use `actorHandle.RemoteCall(methodName, args...) -> ObjectRef` to invoke actor methods,
with usage identical to `ray.RemoteCall()`.

**Retrieving Return Values**

Both task calls and actor method calls return an `ObjectRef`, representing a future for the remote call result.
Since Golang functions/methods support multiple return values, `ObjectRef` also supports multiple return values. There are three ways to retrieve return values from an `ObjectRef`:

One approach is to use `ObjectRef.GetInto(ptrs...) -> error` by passing pointers of corresponding return types.

Another approach is through the `ray.Get0()`, `ray.Get1[T]()`, `ray.Get2[T1, T2]()`, ... series of generic functions:
- If the task/actor method has no return value, use `ObjectRef.Get0() -> error` to wait for task/actor method completion.
- If the task/actor method returns a single value, use `ray.Get1[T]() -> (T, error)` to retrieve it, where `T` corresponds to the return type.
- If there are 2 return values, use `ray.Get2[T1, T2]() -> (T1, T2, error)`, where `T1, T2` correspond to the return types.
- And so on.

The final approach is to use `ObjectRef.GetAll()` to retrieve return values as `[]any` (i.e., an `interface{}` slice), then use type assertions to convert to specific types.

You can pass an `ObjectRef` as a parameter to `RemoteCall()`, but only for single-return-value `ObjectRef`s, and the `ObjectRef` result type must be compatible with the parameter type.

**Parameters and Return Values for Remote Calls**

Parameters and return values for remote calls are serialized and deserialized using [msgpack](https://msgpack.org/).
Supported types include: integers, floats, booleans, strings, binary data, slices, maps, nil, structs, and their pointer types. Note that **map keys only support string types** (not integers).

**Other APIs**

- `Put(data any) -> (SharedObject, error)` places an object into Ray's object store; the returned SharedObject can be passed as a parameter to other tasks/actors.
- `Wait(objRefs []ObjectRef, requestNum int, opts ...*option) ([]ObjectRef, []ObjectRef, error)`
   Waits for a specified number of ObjectRefs to complete, returning slices of completed and uncompleted ObjectRefs.
- `ObjectRef.Cancel(opts ...*option) -> error` terminates execution of a remote task (task/actor method).
- `GetActor(name string, opts ...*option) (*ActorHandle, error)` retrieves a handle to a named actor instance.
- `actorHandle.Kill(opts ...*option)` terminates an actor instance.

Where `*option` represents ray call parameters, created using `ray.Option(name string, val any)`. For specific parameters, refer to the [Ray official API documentation](https://docs.ray.io/en/latest/ray-core/api/core.html).

### Compilation

```bash
go build -buildmode=c-shared -o ./build/raytask .
```

Must use `-buildmode=c-shared` to compile as a shared library.

### Submitting Ray Jobs

#### Ray Standalone Environment

1. Install the official Ray Python SDK:

```bash
pip install -U "ray[default]" 
```

2. Install the go-ray Python driver:
```bash
pip install "git+https://github.com/ray4go/go-ray" 
```

3. Run the ray job:
```bash
goray --mode local ./build/raytask
```

#### Ray Cluster

Prerequisites: Install the go-ray Python driver in the Ray cluster.

Submit a ray job to the Ray cluster using:
```bash
export RAY_ADDRESS="http://RAY_CLUSTER_ADDRESS"  # Replace with your Ray cluster address
ray job submit --working-dir=./ -- goray ./build/raytask
```

Note:
- The compiled binary must be placed in the `working-dir` directory or its subdirectories and cannot be excluded by .gitignore
- The binary path passed to goray must be relative to `working-dir`

<details>
  <summary>Command Explanation</summary>

To run Go in Ray, the compiled binary must first be distributed to all nodes in the cluster. By specifying `--working-dir=./`,
Ray will package and distribute files from the working directory to each node.

`goray` is the Python-written task entry point responsible for communicating with Go. The binary path passed to goray is actually the file's path on the cluster.
Since the working directory for jobs submitted via `ray job submit` cannot be determined in advance, a relative path is required.
If the binary has already been distributed to cluster nodes through other means, an absolute path can be used.

</details>

### Cross-Language Invocation

Cross-language invocation supports:
- Cross-language invocation of Ray remote tasks and actors
  - Golang calling Python-declared ray tasks and actors; Python calling Go-declared ray tasks and actors
  - Golang ObjectRefs passed as parameters to Python tasks/actors; vice versa is also supported
  - Python retrieving named Go ray actor handles; vice versa is also supported
- Local cross-language invocation
  - Golang calling Python functions within the current process
  - Python calling Golang functions within the current process
  - Python initializing Golang classes and calling their methods within the current process

Usage:

Install the GoRay Python SDK:
```bash
pip install "git+https://github.com/ray4go/go-ray" 
```

In Python code:
- Use the `goray.remote` decorator to declare ray tasks/actors, replacing the original `ray.remote`.
- Use `goray.init(libpath: str, **ray_init_args)` to initialize ray, replacing the original `ray.init`.
  - `libpath` is the path to the compiled Go shared library.
  - `ray_init_args` are the ray initialization parameters originally passed to `ray.Init`
- Use `goray.golang_actor_class(name: str)` to get a Golang ray actor class
- Use `goray.golang_task(name: str)` to get a Golang ray task
- Use `goray.golang_local_run_task(name: str, *args)` to call a Golang function within the current process
- Use `goray.golang_local_new_actor(name: str, *args)` to initialize a Golang class within the current process

In Golang code:
- `ray.RemoteCallPyTask(name, args...) -> ObjectRef` calls a Python-declared ray task
- `ray.NewPyActor(name, args...) -> ActorHandle` creates a Python-declared ray actor
- `ray.LocalCallPyTask(name, args...) -> LocalPyCallResult` calls a Python function within the current process

Submit the ray job as you would with a regular Python Ray project.

Demo: [app.go](examples/app.go), [app.py](examples/app.py)

Parameters and return values are serialized and deserialized between Python and Go using [msgpack](https://msgpack.org/).
Supported types include: integers, floats, booleans, strings, binary data, lists (Golang slices), maps, and nil.
Map keys only support string and integer types. The Golang side additionally supports structs and pointer types. For details, see [GoRay Cross-Language Invocation - Type Conversion Guide](docs/crosslang_types.md)

Note:
- When Python calls Go, if the Go task/method has a single return value, Python receives a single value. For multiple return values, Python receives a list
- When Python calls Go, only positional arguments are supported; keyword arguments are not supported
- When Go calls Python, if there is a return value, it is always returned to Go as a single return value in list type. Therefore, you need to use `ray.Get1(objectRef)` or `objectRef.GetInto(&val)` to retrieve the return value (if any)


### Codegen

GoRay provides methods such as ray.RemoteCall(), ray.NewActor(), and actorHandle.RemoteCall() that use strings to specify task/actor method names, with task parameters of type `any` and return values requiring manual type specification.
This prevents compile-time checking of name and parameter type correctness, and IDEs cannot provide code completion features.

GoRay provides a codegen CLI tool `goraygen` to generate high-level wrapper APIs for ray tasks and actors. The wrapper code's parameter types align with the original ray tasks/actors, and the returned futures are type-aware of return values.
Remotely calling ray tasks/actors through wrapper APIs achieves compile-time type safety for remote calls and is IDE-friendly.

Usage:

1. Annotate the ray tasks struct with `// raytasks` and the ray actor factory struct with `// rayactors`.
2. Generate high-level code using:
```bash
go install github.com/ray4go/go-ray/goraygen
goraygen /path/to/your/package/
```

goraygen will generate a `ray_workloads_wrapper.go` file in the package directory containing wrappers for all ray tasks and actors ([example file](./examples/ray_workloads_wrapper.go)).

Usage example:

```golang
future := Devide(16, 5).Remote(ray.Option("num_cpus", 2))
res, remainder, err := future.Get()
```


## Best Practices

Parameter & Return Value Types:
- Can be primitive types, composite types, structs, and their pointer types
- Using `interface{}` as a type is not recommended; members in composite types should also avoid being `interface{}`
- Interface types cannot be used as parameters or return values

Error Handling:
- Do not use the `error` type as a return value (error type is an interface). It's recommended to return integer or string types for error handling

## How It Works

Go supports compilation to shared libraries and exposes C APIs via cgo for cross-language invocation of Golang code. Python supports calling C library APIs through ctypes.
This enables Python to call Golang code. Combined with Golang's reflection mechanism, it's possible to dynamically invoke Golang functions/methods from Python.

By defining C callback functions on the Python side using ctypes and passing them to Golang, Golang can call Python functions.

Using msgpack to serialize parameters and return values enables cross-language parameter passing and return value retrieval between Python and Golang.
