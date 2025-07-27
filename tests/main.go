package main

import (
	"github.com/ray4go/go-ray/ray"
	"github.com/ray4go/go-ray/tests/cases"
	"fmt"
	"github.com/bytedance/mockey"
	"os"
	"regexp"
	"testing"
)

func init() {
	task, actors := cases.RayWorkload()
	ray.Init(driver, task, actors)
}

func driver() {
	// override os.Args
	os.Args = []string{"go", "-test.v"}
	//os.Args = []string{"go", "-test.coverprofile", "/tmp/out.cov"}
	matchAll := func(pat, str string) (bool, error) {
		return regexp.MatchString(pat, str)
	}

	tests := cases.GetTestCases()
	fmt.Printf("%d test cases found\n", len(tests))

	mockey.Mock(os.Exit).Return().Build() // use os.Exit in goray app will cause Segmentation fault
	// benchmarks 和 examples 在这里我们不需要，传入 nil
	// 注意：这个调用会接管程序并最终以 os.Exit 结束，所以它之后的代码不会执行
	testing.Main(matchAll, tests, nil, nil)
}

// main 函数不会被调用，但不可省略
func main() {}
