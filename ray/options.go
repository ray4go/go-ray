package ray

import (
	"encoding/json"
)

type option struct {
	name  string
	value any
}

// Option creates a new ray option with the given name and value.
// The valid option name and value varies by the ray API being called.
func Option(name string, value any) *option {
	return &option{
		name:  name,
		value: value,
	}
}

func (opt *option) Name() string {
	return opt.name
}

func (opt *option) Value() any {
	return opt.value
}

type kv interface {
	Name() string
	Value() any
}

func jsonEncodeOptions[T kv](opts []T, extra ...T) ([]byte, error) {
	kvs := make(map[string]any)
	for _, opt := range opts {
		kvs[opt.Name()] = opt.Value()
	}
	for _, opt := range extra {
		kvs[opt.Name()] = opt.Value()
	}
	return json.Marshal(kvs)
}
