import ast
import inspect
import textwrap
import logging
import time

DEPENDENT_ACCESS_THRESHOLD = 3
ROLLBACK_IDENTIFIER = "UnknownArg"
COMPARE_EXEC_COUNT = 10
ANNA_CLIENT_NAME = 'anna'
SHREDDER_CLIENT_NAME = 'shredder'

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
                            return ROLLBACK_IDENTIFIER
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
    if loop_res and RPN_str:
        # print(f'Node {node} get loop res: {loop_res}')
        RPN_str = " ".join([RPN_str, loop_res, '*'])
    
    # print(f'node: {type(node).__name__:{16}}, fields: {list(ast.iter_fields(node))} RPN: {RPN_str}')
    
    return RPN_str

class Arbiter:
    ## Assume only one function pinned to executor
    def __init__(self):
        # Func meta
        self.func = None
        self.func_name = None
        self.func_ast = None
        self.func_args = None
        self.RPN_str = None
        # Profile
        self.exec_start_time = time.time()  ## There's no parallel execution
        self.latencies = []
        # Rollback case
        self.rollback_flag = False
        self.compare_latencies = { ANNA_CLIENT_NAME: [], SHREDDER_CLIENT_NAME: [] }
        self.compare_decision = None
    
    def clear_compare(self):
        return NotImplementedError

    def bind_func(self, func, func_name):
        logging.info(f'binding func {func_name}')
        self.func = func
        self.func_name = func_name
        self.func_ast = ast.parse(textwrap.dedent(inspect.getsource(func)))
        self.func_args = get_func_args(self.func_ast)
        self.RPN_str = generate_RPN_str(self.func_ast, self.func_args)
        if ROLLBACK_IDENTIFIER in self.RPN_str:
            # Cannot analyze args, rollback: run both sides then compare
            self.rollback_flag = True
        logging.info(f'ARG: {self.func_args}, RPN_str: {self.RPN_str}')
        
    def current_compare_client(self):
        if len(self.compare_latencies[ANNA_CLIENT_NAME]) < COMPARE_EXEC_COUNT:
            return ANNA_CLIENT_NAME
        elif len(self.compare_latencies[SHREDDER_CLIENT_NAME]) < COMPARE_EXEC_COUNT:
            return SHREDDER_CLIENT_NAME
        else:
            return None
        
    def exec_start(self):
        self.exec_start_time = time.time()
    
    def exec_end(self):
        elapsed = time.time() - self.exec_start_time
        if self.rollback_flag and not self.compare_decision:
            self.compare_latencies[self.current_compare_client()].append(elapsed)
        else:
            self.latencies.append(elapsed)
        return elapsed

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
                if elem in arg_map.keys():
                    ## variable
                    stack.append(int(arg_map[elem]))
                else:
                    ## UnknownArg
                    return None
        assert(len(stack) == 1)
        return stack.pop()
    
    def process_args(self, args):
        client_name = args[-1]
        if not isinstance(client_name, str) or client_name != 'arbiter':
            logging.info(f'Arbiter returns original args')
            return args
        
        assert len(args) == len(self.func_args) + 1, f'Final_arg len: {len(args)}, Func_arg len: {len(self.func_args)}'
        
        if self.rollback_flag:
            # Rollback case
            client_arg = self.rollback_compare()
        else:
            # Normal case
            arg_map = {}
            for arg_i, arg in enumerate(self.func_args):
                arg_map[arg] = args[arg_i + 1]
            
            dependent_access_times = self.calc(arg_map)
            if dependent_access_times > DEPENDENT_ACCESS_THRESHOLD:
                client_arg = SHREDDER_CLIENT_NAME
            else:
                client_arg = ANNA_CLIENT_NAME
        
        # Choose the better client
        final_args = args[:-1]
        final_args += (client_arg, )
        
        logging.info(f'Dependent access: {dependent_access_times}, choose: {final_args[-1]}')
        logging.info(f'Args: {args}, Final_args: {final_args}')
        
        return final_args
    
    def rollback_compare(self):
        if self.compare_decision:
            return self.compare_decision
        
        cur_client = self.current_compare_client()
        if cur_client:
            return cur_client
        else:
            # We collected enough latencies for comparison
            return self.compare_choose_client()
    
    def compare_choose_client(self):
        # Choose lower median latency
        anna_median = self.compare_latencies[ANNA_CLIENT_NAME][len(self.compare_latencies[ANNA_CLIENT_NAME]) / 2]
        shredder_median = self.compare_latencies[SHREDDER_CLIENT_NAME][len(self.compare_latencies[SHREDDER_CLIENT_NAME]) / 2]
        if anna_median < shredder_median:
            return ANNA_CLIENT_NAME
        else:
            return SHREDDER_CLIENT_NAME