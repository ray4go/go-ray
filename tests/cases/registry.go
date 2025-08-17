package cases

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

var (
	tests          = make([]testing.InternalTest, 0)
	actorFactories = make(map[string]any)
)

// raytasks
type testTask struct{}

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

// register an actor and return its name.
// need to be called in init()
func RegisterActor(factory any) string {
	name := fmt.Sprintf("actor_%d", len(actorFactories))
	actorFactories[name] = factory
	return name
}

func RegisterNamedActor(name string, factory any) string {
	if ok := actorFactories[name]; ok != nil {
		panic(fmt.Sprintf("actor %s already registered", name))
	}
	actorFactories[name] = factory
	return name
}

func GetTestCases() []testing.InternalTest {
	return tests
}

func RayWorkload() (testTask, map[string]any) {
	return testTask{}, actorFactories
}
