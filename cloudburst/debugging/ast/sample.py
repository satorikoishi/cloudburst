import ast
import inspect

def k_hop(cloudburst, id, k):
        friends = cloudburst.get(id).tolist()
        sum = len(friends)
        
        if k == 1:
            return sum
        
        for friend_id in friends:
            sum += k_hop(cloudburst, friend_id, k - 1)
        
        return sum

res = ast.parse(inspect.getsource(k_hop))
print(ast.dump(res))