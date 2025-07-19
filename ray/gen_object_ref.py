import os
import string

max_returns_len = 8

TPL = """
// Get${l} is used to get the result of remote task / actor method with ${l} return value.
func (obj ObjectRef) Get${l}() (${return_list}, error) {
	res, err := obj.GetAll()
	if err != nil {
		return ${nil_list}, err
	}
	if len(res) != ${l} {
		panic(fmt.Sprintf("ObjectRef.Get${l}: the number of return values error, expect ${l} but got %v", len(res)))
	}
	return ${res_list}, err
}
"""


obj_tpl = string.Template(TPL)

for l in range(1, max_returns_len + 1):
    nil_list = ', '.join(['nil'] * l)
    res_list = ', '.join([f'res[{i}]' for i in range(l)])
    return_list = ', '.join(['any'] * l)
    obj_method = obj_tpl.substitute(l=l, nil_list=nil_list, res_list=res_list, return_list=return_list)
    print(obj_method)

