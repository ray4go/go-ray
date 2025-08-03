# Python & Golang Cross Language Call Framework

Support:
- go call python function
- python call go function
- python init go class and call method

## Usage

[Example](../../examples/crosslang)

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
	"fmt"
	"github.com/ray4go/go-ray/ray"
)

type counter struct {
	num int
}

func NewCounter(n int) *counter {
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

type export struct{}

func (_ export) Echo(args ...any) []any {
	return args
}

func (_ export) Hello(name string) string {
	return fmt.Sprintf("Hello %s", name)
}

func init() {
	ray.Init(export{}, map[string]any{"Counter": NewCounter}, nil)
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

res, err := ray.LocalCallPyTask("echo", 1, "str", []byte("bytes"), []int{1, 2, 3})
fmt.Println("go call python: echo", res, err)

res, err = ray.LocalCallPyTask("hello", "from go")
fmt.Println("go call python: hello", res, err)

res, err = ray.LocalCallPyTask("no_return", "")
fmt.Println("go call python: no_return", res, err)
```

Noted that the `ray.Init()` must be called before `ray.LocalCallPyTask()`.