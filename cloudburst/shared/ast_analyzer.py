import ast

FALLBACK_IDENTIFIER = 'FALLBACK'
ACCESS_FUNC_NAME = 'cloudburst'
SELF_FUNC_NAME = ""    

def check_funcdef_name(node):
    if isinstance(node, ast.FunctionDef):
        # print(f'Found Func Def, return {node.name}')
        return node.name
    else:
        return ""

def check_args(node):
    args = []
    if isinstance(node, ast.FunctionDef):
        for idx, arg in enumerate(node.args.args):
            if idx == 0:
                continue    # ignore arg cloudburst
            args.append(arg.arg)
    return args

def check_call(node, args=[]):
    if isinstance(node, ast.Call):
        if isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name):
                # Check access call
                if node.func.value.id == ACCESS_FUNC_NAME:
                    # print(f'node: {type(node).__name__:{16}}, fields: {list(ast.iter_fields(node))}')
                    return "1"
        elif isinstance(node.func, ast.Name):
            # Check recursive call
            if node.func.id == SELF_FUNC_NAME:
                return FALLBACK_IDENTIFIER
    
    return ""

def check_loop(node, args=[]):
    if isinstance(node, ast.For):        
        if isinstance(node.iter, ast.Call):
            if isinstance(node.iter.func, ast.Name):
                # print(f'func id: {node.iter.func.id}, args: {node.iter.args}')
                if node.iter.func.id == 'range':                    
                    ## A for range loop, we can analyze loop times
                    ## Only consider arg0
                    range_arg0 = node.iter.args[0]
                    if isinstance(range_arg0, ast.Constant):
                        # print(f'node: {type(node).__name__:{16}}, fields: {list(ast.iter_fields(node))}')
                        return str(range_arg0.value)
                    elif isinstance(range_arg0, ast.Name):
                        # print(f'node: {type(node).__name__:{16}}, fields: {list(ast.iter_fields(node))}')
                        if range_arg0.id in args:
                            return range_arg0.id
                        else:
                            return FALLBACK_IDENTIFIER
    return ""

def get_funcdef_name(func_ast):
    global SELF_FUNC_NAME
    for node in ast.walk(func_ast):
        SELF_FUNC_NAME = check_funcdef_name(node)
        if SELF_FUNC_NAME:
            return

def get_func_args(func_ast):
    args = []
    for node in ast.walk(func_ast):
        args = check_args(node)
        if len(args) > 0:
            break
    return args

def generate_RPN_str(node, args=[]):
    # print(f'Nodetype: {type(node).__name__:{16}} {node}')
    RPN_str = ""
    
    ## If call, it is the most underlying layer, no need to check child
    call_res = check_call(node, args)
    if call_res:
        # print(f'Node {node} get call res: {call_res}')
        return call_res
    
    for child in ast.iter_child_nodes(node):
        child_RPN_str = generate_RPN_str(child, args)
        if child_RPN_str:
            if RPN_str:
                RPN_str = " ".join([RPN_str, child_RPN_str, '+'])
            else:
                RPN_str = child_RPN_str
    
    loop_res = check_loop(node, args)
    if loop_res and RPN_str:
        # print(f'Node {node} get loop res: {loop_res}')
        RPN_str = " ".join([RPN_str, loop_res, '*'])
    
    # print(f'node: {type(node).__name__:{16}}, fields: {list(ast.iter_fields(node))} RPN: {RPN_str}')
    
    return RPN_str

def calc(RPN_str, arg_map):
    RPN_list = RPN_str.split()
    # print(RPN_list)
    
    stack = []
    for elem in RPN_list:
        if elem == '+' or elem == '*':
            ## operator
            right = stack.pop()
            left = stack.pop()
            if elem == '+':
                stack.append(left + right)
            else:
                stack.append(left * right)
        elif elem.isnumeric():
            ## const num
            stack.append(int(elem))
        else:
            if elem in arg_map.keys():
                ## variable
                stack.append(int(arg_map[elem]))
            else:
                ## UnknownArg
                return None
    assert(len(stack) == 1)
    return stack.pop()

if __name__ == '__main__':
    pass