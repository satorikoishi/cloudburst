import ast
import inspect
import logging

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
            if node.func.value.id == 'cloudburst':
                # print(f'node: {type(node).__name__:{16}}, fields: {list(ast.iter_fields(node))}')
                return "1"
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
                            return "UnknownArg"
    return ""

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
    if loop_res:
        # print(f'Node {node} get loop res: {loop_res}')
        RPN_str = " ".join([RPN_str, loop_res, '*'])
    
    # print(f'node: {type(node).__name__:{16}}, fields: {list(ast.iter_fields(node))} RPN: {RPN_str}')
    
    return RPN_str

class Arbiter:
    ## Assume only one function pinned to executor
    def __init__(self):
        self.func = None
        self.func_name = None
        self.func_ast = None
        self.func_args = None
        self.RPN_str = None

    def bind_func(self, func_name, func):
        logging.info(f'binding func {func_name}')
        self.func = func
        self.func_name = func_name
        self.func_ast = ast.parse(inspect.getsource(func))
        self.func_args = get_func_args(self.func_ast)
        self.RPN_str = generate_RPN_str(self.func_ast, self.func_args)
        logging.info(f'ARG: {self.func_args}, RPN_str: {self.RPN_str}')

    def calc(self, arg_map):
        RPN_list = self.RPN_str.split()
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
                ## variable
                stack.append(int(arg_map[elem]))
        assert(len(stack) == 1)
        return stack.pop()