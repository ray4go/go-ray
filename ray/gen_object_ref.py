import string
import textwrap

max_returns_len = 16

TPL = """
// Get${l} is used to get the result of task / actor method with ${l} return value.
// [WithTimeout]() can be used as GetObjectOption to set timeout. 
func Get${l}[${T_any_list}](obj Decodable, options ...GetObjectOption) (${T_list}, error) {
	var (
${var_list}
	)
	err := obj.GetInto(appendOptions(options, ${addr_list})...)
	return ${val_list}, err
}
"""

obj_tpl = string.Template(TPL)

for l in range(1, max_returns_len + 1):
    T_any_list = ", ".join(f"T{i} any" for i in range(l))  # T0 any, T1 any, ..., Tn any
    T_list = ", ".join(f"T{i}" for i in range(l))  # T0, T1,..., Tn
    var_list = "\n".join(f"r{i} T{i}" for i in range(l))  # r0 T0, r1 T1,..., rn Tn
    addr_list = ", ".join(f"&r{i}" for i in range(l))  # &r0, &r1,..., &rn
    val_list = ", ".join(f"r{i}" for i in range(l))  # r0, r1,..., rn

    var_list = textwrap.indent(var_list, "\t\t")
    obj_method = obj_tpl.substitute(
        l=l,
        T_any_list=T_any_list,
        T_list=T_list,
        var_list=var_list,
        addr_list=addr_list,
        val_list=val_list,
    )
    print(obj_method)
