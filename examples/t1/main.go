package main

import (
	"github.com/ray4go/go-ray/ray/pkg/common"
	"fmt"
	"log"
	"time"

	"github.com/ray4go/go-ray/ray"
)

func init() {
	ray.Init(driver, demo{}, map[string]any{"Counter": NewActor}) // 初始化，注册ray driver, ray tasks 和 actors
}

func driver() {
	// ray task
	answerObjRef := ray.RemoteCall("TheAnswerOfWorld")
	objRef := ray.RemoteCall("Divide", answerObjRef, 5, common.Option("num_cpus", 2))
	res, remainder, err := objRef.Get2()
	if err != nil {
		log.Panicf("remote task error: %v", err)
	}
	fmt.Printf("call Divide -> %#v, %#v \n", res, remainder)

	// ray actor
	cnt := ray.NewActor("Counter", 1)
	obj := cnt.RemoteCall("Incr", 1)
	res2, err2 := obj.Get1()
	fmt.Println("Incr ", res2, err2)

	obj2 := cnt.RemoteCall("Incr", 1)
	obj3 := cnt.RemoteCall("Incr", obj2)
	fmt.Println(obj3.Get1())
}

// raytasks
type demo struct{}

func (_ demo) TheAnswerOfWorld() int64 {
	time.Sleep(1 * time.Second)
	return 42
}

func (_ demo) Divide(a, b int64) (int64, int64) {
	return a / b, a % b
}

// ray actor
type counter struct {
	num int
}

func NewActor(n int) *counter {
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

// main 函数不会被调用，但不可省略
func main() {}
