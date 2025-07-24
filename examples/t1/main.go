package main

import (
	"fmt"
	"log"

	"github.com/ray4go/go-ray/ray"
)

func init() {
	ray.Init(driver, demo{}) // 初始化，注册ray driver 和 tasks
}

func driver() {
	objRef := ray.RemoteCall("Devide", 16, 5, ray.WithTaskOption("num_cpus", 2)) // ray async remote call
	res, remainder, err := objRef.Get2()
	if err != nil {
		log.Panicf("remote task error: %v", err)
	}
	fmt.Printf("call Devide -> %#v, %#v \n", res, remainder)

	{
		future := demoTasks.Devide(16, 5).Remote(ray.WithTaskOption("num_cpus", 2))
		res, remainder, err := future.Get()
		if err != nil {
			log.Panicf("remote task error: %v", err)
		}
		fmt.Printf("call Devide -> %#v, %#v \n", res, remainder)
	}
}

// raytasks
type demo struct{}

func (_ demo) Devide(a, b int64) (int64, int64) {
	return a / b, a % b
}

// main 函数不会被调用，但不可省略
func main() {}
