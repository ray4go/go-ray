package main

import (
	"fmt"
	"log"
)

func assert(a, b any) {
	if a != b {
		log.Printf("assert failed: %#v != %#v", a, b)
		panic("assert failed")
	}
}

func init() {
	Init(driver, add0, task1, convert2)
}

func driver() {
	f1 := RemoteCall(0, 2, 3)
	res := Get(f1)
	assert(res, 5)

	f2 := RemoteCall(1, 2)
	res2 := Get(f2)
	fmt.Println(res2)
}

func add0(a, b int) int {
	return a + b
}

func task1(n int) string {
	res := ""
	for i := 0; i < n; i++ {
		res += convert2(i, i+1)
	}
	return res
}

func convert2(a, b int) string {
	return fmt.Sprintf("%d*%d=%d ", a, b, a*b)
}

func main() {
	fmt.Printf("[Go] main\n")
}
