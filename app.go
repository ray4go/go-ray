package main

import (
	"fmt"
	"log"
	"math"
	"os"
)

func assert(a, b any) {
	if a != b {
		log.Printf("assert failed: %#v != %#v", a, b)
		panic("assert failed")
	}
}

type Point struct {
	X, Y int
}

type Error string

func init() {
	Init(driver2, export{})
	// gob.Register(Point{})
}

func driver2() {

	res, err := CallPythonCode("put(111)")
	fmt.Println("res:", res, err)

	res1 := RemoteCall("Hello", "2cpu", WithTaskOption("num_cpus", 2))
	res2 := RemoteCall("Hello", "1cpu", WithTaskOption("num_cpus", 1))
	r, err := res1.Get()
	fmt.Printf("res1: %#v  %#v\n", r, err)
	fmt.Printf("res2: %#v\n", res2.Result())

	res := RemoteCall("MultiReturn")
	// fmt.Printf("res: %#v\n", Get(res))
	a, b, err := res.Get2()
	fmt.Printf("a: %#v, b: %#v  err:%v\n", a, b, err)

	res = RemoteCall("Error")
	// fmt.Printf("res: %#v\n", Get(res).(Error))
}

type export struct{}

func (_ export) Hello(info string) string {
	// panic("test panic")
	host, _ := os.Hostname()
	fmt.Printf("[%s] %s\n", host, info)
	return info
}

func (_ export) Nil(a, b int64) {
	fmt.Println("Nil")
	return
}

func (_ export) Error() Error {
	return Error("test error")
}

func (_ export) MultiReturn() (int, string) {
	return 2, "test"
}

func (_ export) Add(a, b int64) int64 {
	return a + b
}

func (_ export) Task(n int) string {
	res := ""
	for i := 0; i < n; i++ {
		res_ref := RemoteCall("Convert", i, i+1)
		res += res_ref.Result()[0].(string)
	}
	return res
}

func (_ export) Convert(a, b int) string {
	return fmt.Sprintf("%d*%d=%d ", a, b, a*b)
}

func (_ export) Distance(a, b Point) float64 {
	dis := (a.X-b.X)*(a.X-b.X) + (a.Y-b.Y)*(a.Y-b.Y)
	return math.Sqrt(float64(dis))
}

func (_ export) AddPoints(points []Point) Point {
	host, _ := os.Hostname()
	fmt.Println("remote AddPoints", host)
	res := Point{}
	for _, p := range points {
		res.X += p.X
		res.Y += p.Y
	}
	return res
}

func main() {
	fmt.Printf("-------- [Go] main --------  \n")
}
