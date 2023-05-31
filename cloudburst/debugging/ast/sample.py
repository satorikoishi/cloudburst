import ast
from cloudburst.shared.serializer import Serializer

serializer = Serializer()

def k_hop(cloudburst, id, k):
        friends = cloudburst.get(id).tolist()
        sum = len(friends)
        
        if k == 1:
            return sum
        
        for friend_id in friends:
            sum += k_hop(cloudburst, friend_id, k - 1)
        
        return sum

body = serializer.dump(k_hop)

res = ast.parse(serializer.load(body))
print(res)