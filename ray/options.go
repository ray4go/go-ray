package ray

import (
	"encoding/json"
)

type option struct {
	name  string
	value any
}

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

type KV interface {
	Name() string
	Value() any
}

func JsonEncodeOptions[T KV](opts []T, extra ...T) ([]byte, error) {
	kvs := make(map[string]any)
	for _, opt := range opts {
		kvs[opt.Name()] = opt.Value()
	}
	for _, opt := range extra {
		kvs[opt.Name()] = opt.Value()
	}
	return json.Marshal(kvs)
}
