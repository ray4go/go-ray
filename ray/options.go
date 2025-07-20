package ray

type Option struct {
	name  string
	value any
}

func NewOption(name string, value any) *Option {
	return &Option{
		name:  name,
		value: value,
	}
}

func (opt *Option) Name() string {
	return opt.name
}

func (opt *Option) Value() any {
	return opt.value
}

type KV interface {
	Name() string
	Value() any
}

func EncodeOptions[T KV](opts []T) map[string]any {
	kvs := make(map[string]any)
	for _, opt := range opts {
		kvs[opt.Name()] = opt.Value()
	}
	return kvs
}
