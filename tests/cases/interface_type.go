package cases

import (
	"encoding/json"
	"github.com/ray4go/go-ray/ray"
	"github.com/stretchr/testify/require"
)

func (TestTask) AnyTypeParamAndRet(args ...any) (any, any) {
	return args[0], args[1]
}

func (TestTask) ReturnInterface(jsonStr string) (any, error) {
	var res any
	err := json.Unmarshal([]byte(jsonStr), &res)
	return res, err
}

func init() {
	AddTestCase("TestAnyTypeParamAndRet", func(assert *require.Assertions) {
		objRef := ray.RemoteCall("AnyTypeParamAndRet", 123, "hello")
		objRef.DisableAutoRelease()
		res1, res2, err := ray.Get2[int, string](objRef)
		assert.NoError(err)
		assert.Equal(123, res1)
		assert.Equal("hello", res2)

		res3, res4, err := ray.Get2[any, any](objRef)
		assert.NoError(err)
		assert.Equal(int64(123), res3)
		assert.Equal("hello", res4)

		objRef2 := ray.RemoteCall("AnyTypeParamAndRet", nil, 123)
		res5, res6, err := ray.Get2[any, int](objRef2)
		assert.Nil(res5)
		assert.Equal(123, res6)
		assert.NoError(err)

		objRef3 := ray.RemoteCall("ReturnInterface", "1")
		res7, errBiz, err := ray.Get2[int, error](objRef3)
		assert.Equal(1, res7)
		assert.NoError(err)
		assert.NoError(errBiz)
	})
}
