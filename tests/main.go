package main

import (
	"github.com/ray4go/go-ray/ray"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"os"
	"regexp"
	"testing"
)

var tests = make([]testing.InternalTest, 0)

// raytasks
type testTask struct{}

func init() {
	ray.Init(driver, testTask{}) // 初始化，注册ray driver 和 tasks
}

func addTestCase(name string, f func(*require.Assertions)) {
	testFunc := func(t *testing.T) {
		assert := require.New(t)
		f(assert)
	}
	tests = append(tests, testing.InternalTest{
		Name: name,
		F:    testFunc,
	})
}

func driver() {
	// override os.Args
	os.Args = []string{"go", "-test.v"}
	//os.Args = []string{"go", "-test.coverprofile", "/tmp/out.cov"}
	matchAll := func(pat, str string) (bool, error) {
		return regexp.MatchString(pat, str)
	}

	mockey.Mock(os.Exit).Return().Build() // use os.Exit in goray app will cause Segmentation fault
	// benchmarks 和 examples 在这里我们不需要，传入 nil
	// 注意：这个调用会接管程序并最终以 os.Exit 结束，所以它之后的代码不会执行
	testing.Main(matchAll, tests, nil, nil)
}

// main 函数不会被调用，但不可省略
func main() {}
