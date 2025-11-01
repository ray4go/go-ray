package remote_call

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSomething(t *testing.T) {
	assert := require.New(t)

	type T1 struct {
		A int
		B string
		C []byte
	}
	t1 := T1{A: 1, B: "str", C: []byte("bytes")}
	data := EncodeSlice([]any{t1})

	var t2 T1
	err := DecodeInto(data, []any{&t2})
	assert.Nil(err)
	assert.Equal(t1, t2)
}
