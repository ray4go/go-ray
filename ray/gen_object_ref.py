import string

max_returns_len = 5

TPL = """
// Get${l} is used to get the result of remote task / actor method with ${l} return value, with optional timeout.
func Get${l}[${T_any_list}](obj ObjectRef, timeout ...float64) (${T_list}, error) {
	r, err := getN(obj, ${l}, timeout...)
	if err!= nil {
		return ${zero_list}, err
	}
	return ${ret_asserts}, err
}
"""


obj_tpl = string.Template(TPL)

for l in range(1, max_returns_len + 1):
    T_any_list = ", ".join(f"T{i} any" for i in range(l))  # T0 any, T1 any, ..., Tn any
    T_list = ", ".join(f"T{i}" for i in range(l))  # T0, T1,..., Tn
    ret_asserts = ", ".join(
        f"r[{i}].(T{i})" for i in range(l)
    )  # r[0].(T0), r[1].(T1),..., r[n].(Tn)
    zero_list = ", ".join(
        f"*new(T{i})" for i in range(l)
    )  # *new(T0), *new(T1),..., *new(Tn)

    obj_method = obj_tpl.substitute(
        l=l,
        T_any_list=T_any_list,
        T_list=T_list,
        ret_asserts=ret_asserts,
        zero_list=zero_list,
    )
    print(obj_method)
