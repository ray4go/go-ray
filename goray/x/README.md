# Python & Golang Cross Language Call Framework

Supports:

- Go call Python function
- Python call Go function
- Python init Go class and call method

Python调用go时，只支持通过位置参数传递参数，不支持通过关键字参数传递参数

For the type convertion of parameters and return values when cross language call,
see [Types convertion](../../docs/crosslang_types.md)

Notice:
由于Golang函数使用的是virtual stack而不是system stack，其他语言通过cgo调用Golang函数时，存在约 30ns 的固定开销（比
C、C++、Rust等语言高一个数量级）。
因此Golang的跨语言调用不适用于高频短调用（Fast Calls）场景。

## Usage

**Export python function:**

```python
from goray import x


@x.export
def echo(*args) -> list:
    return list(args)


@x.export
def hello(name: str):
    return f"hello {name}"
```

**Export golang function and class:**

```go
package main

import (
	"github.com/ray4go/go-ray/ray"
	"fmt"
)

type counter struct {
	num int
}

type exportClass struct{}

func (exportClass) Counter(n int) *counter {
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

type exportFunc struct{}

func (exportFunc) Echo(args ...any) []any {
	return args
}

func (exportFunc) Hello(name string) string {
	return fmt.Sprintf("Hello %s", name)
}

func init() {
	ray.Init(exportFunc{}, exportClass{}, nil)
}

// main function will not be called, but cannot be omitted
func main() {}
```

**Call Golang from Python:**

Build golang lib:

```bash
go build -buildmode=c-shared -o out/go.lib -gcflags="all=-l -N" .
```

```python
from goray import x

lib = x.load_go_lib("out/go.lib")
res = lib.func_call("Echo", 1, "str", b"bytes", [1, 2], {"k": 3})
print(f"go Echo() return: {res}")

counter = lib.new_type("Counter", 1)
res = counter.Incr(10)
print(f"go counter.Incr() return: {res}")
```

**Call Python from Golang:**

```go
import (
"fmt"
"github.com/ray4go/go-ray/ray"
)

res, err := ray.LocalCallPyTask("echo", 1, "str", []byte("bytes"), []int{1, 2, 3}).Get()
fmt.Println("go call python: echo", res, err)

var msg string
err = ray.LocalCallPyTask("hello", "from go").GetInto(&msg)
fmt.Println("go call python: hello", msg, err)

_, err = ray.LocalCallPyTask("no_return", "").Get()
fmt.Println("go call python: no_return", err)
```

Noted that the `ray.Init()` must be called before `ray.LocalCallPyTask()`.