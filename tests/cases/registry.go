package cases

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	tests = make([]testing.InternalTest, 0)
)

// raytasks
type TestTask struct{}

// ActorFactories holds all actor constructor methods
// rayactors
type ActorFactories struct{}

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

func RayWorkload() (TestTask, ActorFactories) {
	return TestTask{}, ActorFactories{}
}
