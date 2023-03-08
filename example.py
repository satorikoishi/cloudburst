from cloudburst.client.client import CloudburstConnection
from cloudburst.shared.reference import CloudburstReference

local_cloud = CloudburstConnection('127.0.0.1', '127.0.0.1', local=True)

# put and get states
for i in range(5):
    print(f"putting value of key {i}: {i*2}")
    local_cloud.put_object(i, i*2)

for i in range(5):
    result = local_cloud.get_object(i)
    print(f"receive value of key {i}: {result}")

# register
sq = local_cloud.register(lambda _, x: x * x, 'square')

# stateless
print(f'result of stateless function call: {sq(3).get()}') # 9

# stateful arguments
local_cloud.put_object('key', 2)
reference = CloudburstReference('key', True)
print(f'result of function call with reference arguments: {sq(reference).get()}') # 4
