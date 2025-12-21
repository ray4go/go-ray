package cases

import (
	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
	"os"
)

const testEnvName = "GORAY_TEST_ENV_VAR"
const testEnvValue = "example_value"

var RayInitOptions = []*ray.RayOption{
	ray.Option("runtime_env", map[string]map[string]string{"env_vars": {testEnvName: testEnvValue}}),
}

func (TestTask) GetEnv(name string) string {
	return os.Getenv(name)
}

func init() {
	AddTestCase("TestInitOptions", func(assert *require.Assertions) {
		obj := ray.RemoteCall("GetEnv", testEnvName)
		result, err := ray.Get1[string](obj)
		assert.NoError(err)
		assert.Equal(testEnvValue, result)
	})
}
