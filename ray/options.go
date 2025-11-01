package ray

import (
	"encoding/json"
)

// RayOption represents a single option to be passed to a Ray API call.
type RayOption struct {
	name  string
	value any
}

// Option creates a new Ray option with the given name and value.
// Valid option names and values depend on the GoRay API being called.
func Option(name string, value any) *RayOption {
	return &RayOption{
		name:  name,
		value: value,
	}
}

func jsonEncodeOptions(opts []*RayOption, extra ...*RayOption) ([]byte, error) {
	kvs := make(map[string]any)
	for _, opt := range opts {
		kvs[opt.name] = opt.value
	}
	for _, opt := range extra {
		kvs[opt.name] = opt.value
	}
	return json.Marshal(kvs)
}
