package ray

import (
	"encoding/json"
)

type RayOption struct {
	name  string
	value any
}

// Option creates a new ray RayOption with the given name and value.
// The valid RayOption name and value varies by the ray API being called.
func Option(name string, value any) *RayOption {
	return &RayOption{
		name:  name,
		value: value,
	}
}

func (opt *RayOption) Name() string {
	return opt.name
}

func (opt *RayOption) Value() any {
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
