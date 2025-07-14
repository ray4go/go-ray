package generic

func ExtendArgs[T any](s1 []any, s2 []T) []any {
	for _, v := range s2 {
		s1 = append(s1, v)
	}
	return s1
}
