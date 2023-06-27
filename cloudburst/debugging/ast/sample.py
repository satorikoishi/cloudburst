from _ast import AST, FunctionDef
import ast
import inspect
from typing import Any
import textwrap
import cloudburst.shared.ast_analyzer as ast_analyzer

def prettify(ast_tree_str, indent=4):
    ret = []
    stack = []
    in_string = False
    curr_indent = 0

    for i in range(len(ast_tree_str)):
        char = ast_tree_str[i]
        if in_string and char != '\'' and char != '"':
            ret.append(char)
        elif char == '(' or char == '[':
            ret.append(char)

            if i < len(ast_tree_str) - 1:
                next_char = ast_tree_str[i+1]
                if next_char == ')' or next_char == ']':
                    curr_indent += indent
                    stack.append(char)
                    continue

            print(''.join(ret))
            ret.clear()

            curr_indent += indent
            ret.append(' ' * curr_indent)
            stack.append(char)
        elif char == ',':
            ret.append(char)

            print(''.join(ret))
            ret.clear()
            ret.append(' ' * curr_indent)
        elif char == ')' or char == ']':
            ret.append(char)
            curr_indent -= indent
            stack.pop()
        elif char == '\'' or char == '"':

            if (len(ret) > 0 and ret[-1] == '\\') or (in_string and stack[-1] != char):
                ret.append(char)
                continue

            if len(stack) > 0 and stack[-1] == char:
                ret.append(char)
                in_string = False
                stack.pop()
                continue

            in_string = True
            ret.append(char)
            stack.append(char)
        elif char == ' ':
            pass
        else:
            ret.append(char)

    print(''.join(ret))
    
# class MyVisitor(ast.NodeVisitor):
#     def generic_visit(self, node):
#         print(f'Nodetype: {type(node).__name__:{16}} {node}')
#         ast.NodeVisitor.generic_visit(self, node)
# visitor = MyVisitor()

# class SearchVisitor(ast.NodeVisitor):
#     def generic_visit(self, node):
#         visit_call(node)
#         ast.NodeVisitor.generic_visit(self, node)
# s_visitor = SearchVisitor()

# class ArgVisitor(ast.NodeVisitor):
#     def visit_FunctionDef(self, node: FunctionDef):
#         res = []
#         for idx, arg in enumerate(node.args.args):
#             if idx == 0:
#                 continue    # ignore arg cloudburst
#             print(f'arg{idx}: {arg.arg}')
#             res.append(arg.arg)
        
# a_visitor = ArgVisitor()

## k hop
def k_hop(cloudburst, id, k):
        friends = cloudburst.get(id).tolist()
        sum = len(friends)
        
        if k == 1:
            return sum
        
        for friend_id in friends:
            sum += k_hop(cloudburst, friend_id, k - 1)
        
        return sum

res = ast.parse(inspect.getsource(k_hop))
prettify(ast.dump(res))

args = ast_analyzer.get_func_args(res)
ast_analyzer.get_funcdef_name(res)

RPN_str = ast_analyzer.generate_RPN_str(res, args)
arg_map = {'id': 1, 'k': 2}
call_count = ast_analyzer.calc(RPN_str, arg_map)
print(f'RPN: {RPN_str}, FuncDef name: {ast_analyzer.SELF_FUNC_NAME}, args: {arg_map}, call {call_count} times')

# print('Using NodeVisitor (depth first):')
# visitor.visit(res)

# print('\nWalk()ing the tree breadth first:')
# for node in ast.walk(res):
#     print(f'Nodetype: {type(node).__name__:{16}} {node}')

# print('Search for function calls')
# s_visitor.visit(res)

print('----------------------------------------------------- k hop end, list traversal now -----------------------------------------------------------')

## list traversal
def list_traversal(cloudburst, nodeid, depth, client_name):
    nodeid += cloudburst.get(nodeid);
    for i in range(depth):
        # nodeid = cloudburst.get(nodeid, client_name)[0]
        nodeid_list = cloudburst.get(nodeid, client_name)
        node_id = nodeid_list[0]
    for i in range(3):
        nodeid += cloudburst.get(nodeid);
    nodeid += cloudburst.get(nodeid);
    for i in range(depth):
        pass
    
    return nodeid

res = ast.parse(inspect.getsource(list_traversal))
prettify(ast.dump(res))

# print('Using NodeVisitor (depth first):')
# visitor.visit(res)

# print('\nWalk()ing the tree breadth first:')
# for node in ast.walk(res):
#     print(f'Nodetype: {type(node).__name__:{16}} {node}')

print('Get args')
# a_visitor.visit(res)
args = []
for node in ast.walk(res):
    args = ast_analyzer.check_args(node)
    if len(args) > 0:
        break
print(args)

# print('Search for function calls')
# s_visitor.visit(res)

## Input: args array, Output: call count array, len = len(args) + 1
## Call count = [count * arg] + fixed times
## Check get/put key, if same key, count as cache?

# print(list(ast.iter_child_nodes(res)))

RPN_str = ast_analyzer.generate_RPN_str(res, args)
arg_map = {'nodeid': 1, 'depth': 32, 'client_name': 'a'}
call_count = ast_analyzer.calc(RPN_str, arg_map)
print(f'RPN: {RPN_str}, args: {arg_map}, call {call_count} times')

# def nested_func():
#     ''' REGISTER FUNCTIONS '''
#     def nested_sample(cloudburst, nodeid, depth):
#         for i in range(depth):
#             nodeid = cloudburst.get(nodeid)[0]
#         return nodeid

#     return nested_sample

# res = ast.parse(textwrap.dedent(inspect.getsource(nested_func())))
# prettify(ast.dump(res))
# res = ast.parse(textwrap.dedent(inspect.getsource(nested_func)))
# prettify(ast.dump(res))