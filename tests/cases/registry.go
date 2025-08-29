package cases

import (
	"github.com/stretchr/testify/require"
	"testing"
)

var (
	tests = make([]testing.InternalTest, 0)
)

// raytasks
type testTask struct{}

// actorFactories holds all actor constructor methods
type actorFactories struct{}

func AddTestCase(name string, f func(*require.Assertions)) {
	testFunc := func(t *testing.T) {
		assert := require.New(t)
		f(assert)
	}
	tests = append(tests, testing.InternalTest{
		Name: name,
		F:    testFunc,
	})
}

func GetTestCases() []testing.InternalTest {
	return tests
}

func RayWorkload() (testTask, actorFactories) {
	return testTask{}, actorFactories{}
}
