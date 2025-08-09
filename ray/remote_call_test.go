package ray

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSomething(t *testing.T) {
	assert := require.New(t)

	type T1 struct {
		A int
		B string
		C []byte
	}
	t1 := T1{A: 1, B: "str", C: []byte("bytes")}
	data := encodeSlice([]any{t1})

	var t2 T1
	err := decodeInto(data, []any{&t2})
	assert.Nil(err)
	assert.Equal(t1, t2)
}
