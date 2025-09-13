package main

import (
	"fmt"
	"time"

	"github.com/ray4go/go-ray/ray"
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

func (exportFunc) Busy(name string, duration time.Duration) string {
	fmt.Println("BusyTask", name, "started at", time.Now())
	time.Sleep(duration * time.Second)
	fmt.Println("BusyTask", name, "finished at", time.Now())
	return fmt.Sprintf("BusyTask %s success", name)
}

func (exportFunc) NoReturnVal(a, b int64) {
	return
}

func (exportFunc) CallPython() {
	res, err := ray.LocalCallPyTask("echo", 1, "str", []byte("bytes"), []int{1, 2, 3}).Get()
	fmt.Println("go call python: echo", res, err)
	res, err = ray.LocalCallPyTask("hello", "from go").Get()
	fmt.Println("go call python: hello", res, err)
	res, err = ray.LocalCallPyTask("no_return", "").Get()
	fmt.Println("go call python: no_return", res, err)
}

func init() {
	ray.Init(exportFunc{}, exportClass{}, nil)
}

// main 函数不会被调用，但不可省略
func main() {}
