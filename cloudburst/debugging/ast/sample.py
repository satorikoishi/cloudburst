import ast
import inspect

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

def k_hop(cloudburst, id, k):
        friends = cloudburst.get(id).tolist()
        sum = len(friends)
        
        if k == 1:
            return sum
        
        for friend_id in friends:
            sum += k_hop(cloudburst, friend_id, k - 1)
        
        return sum

res = ast.parse(inspect.getsource(k_hop))
# prettify(ast.dump(res))

class MyVisitor(ast.NodeVisitor):
    def generic_visit(self, node):
        print(f'Nodetype: {type(node).__name__:{16}} {node}')
        ast.NodeVisitor.generic_visit(self, node)

visitor = MyVisitor()
print('Using NodeVisitor (depth first):')
visitor.visit(res)

print('\nWalk()ing the tree breadth first:')
for node in ast.walk(res):
    print(f'Nodetype: {type(node).__name__:{16}} {node}')
    
class SearchVisitor(ast.NodeVisitor):
    def generic_visit(self, node):
        if isinstance(node, ast.Call):
            for child in ast.iter_child_nodes(node):
                if isinstance(child, ast.Attribute) and isinstance(child.value, ast.Name) and child.value.id == 'cloudburst':
                    prettify(ast.dump(child))

        ast.NodeVisitor.generic_visit(self, node)

print('Search for function calls')
s_visitor = SearchVisitor()
s_visitor.visit(res)
