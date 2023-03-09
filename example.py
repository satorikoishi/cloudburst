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
key_ref = CloudburstReference('key', True)
print(f'result of function call with reference arguments: {sq(key_ref).get()}') # 4

# dag
local_cloud.register_dag('dag', ['square'], [])
local_cloud.put_object('dag_param', 10)
dag_param_ref = CloudburstReference('dag_param', True)
res = local_cloud.call_dag('dag', { 'square': [dag_param_ref] }).get()
print(f'result of DAG call: {res}') # 100
