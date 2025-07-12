package main

import (
	"fmt"
	"log"
	"math"
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

func init() {
	Init(driver, export{})
	// gob.Register(Point{})
}

func driver() {
	point := RemoteCall("AddPoints", []Point{{1, 2}, {3, 4}})
	fmt.Println("AddPoints:", Get(point))
	dis := RemoteCall("Distance", Point{1, 2}, Point{3, 4})
	fmt.Println("Distance:", Get(dis))

	f1 := RemoteCall("Add", 2, 3)
	res := Get(f1)
	assert(res, int64(5))

	f2 := RemoteCall("Task", 2)
	res2 := Get(f2)
	fmt.Println(res2)
}

type export struct{}

func (_ export) Add(a, b int64) int64 {
	return a + b
}

func (_ export) Task(n int) string {
	res := ""
	for i := 0; i < n; i++ {
		res_ref := RemoteCall("Convert", i, i+1)
		res += Get(res_ref).(string)
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
