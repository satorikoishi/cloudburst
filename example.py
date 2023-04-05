from cloudburst.client.client import CloudburstConnection
from cloudburst.shared.reference import CloudburstReference

local_cloud = CloudburstConnection('127.0.0.1', '127.0.0.1', local=True)

print(f'states client names {local_cloud.get_states_client_names()}')
print(f'states client types {local_cloud.get_states_client_types()}')

# put and get states
for i in range(5):
    print(f"putting value of key {i}: {i*5}")
    local_cloud.put_object(i, i*5)

for i in range(5):
    result = local_cloud.get_object(i)
    print(f"receive value of key {i}: {result}")

# register
sq = local_cloud.register(lambda _, x: x * x, 'square')

# stateless
print(f'result of stateless function call: {sq(3).get()}') # 9

# stateful arguments
local_cloud.put_object('100', 2)
key_ref = CloudburstReference('100', True)
print(f'result of function call with reference arguments: {sq(key_ref).get()}') # 4

# dag
local_cloud.register_dag('dag', ['square'], [])
local_cloud.put_object('200', 10)
dag_param_ref = CloudburstReference('200', True)
res = local_cloud.call_dag('dag', { 'square': [dag_param_ref] }).get()
print(f'result of DAG call: {res}') # 100

# user_library
def library_test(user_lib, x):
    res = user_lib.get(x)
    user_lib.put(x, res+100)
    return res
lib_test = local_cloud.register(library_test, 'lib_test')
res = lib_test(key_ref).get()
print(f'result of get key 2 (resolved from key_ref) using user_library: {res}') # 10
print(f"value of key 2 after library_test: {local_cloud.get_object(2)}") # 110

# execute js function remotely
def shredder_example(user_lib):
    results_list = []
    try:
        results_list.append(user_lib.execute_js_fun('setup'))
        results_list.append(user_lib.execute_js_fun('get', '0'))
        results_list.append(user_lib.execute_js_fun('count_friend_list', '0', '1'))
        results_list.append(user_lib.execute_js_fun('predict'))
        results_list.append(user_lib.execute_js_fun('list_traversal', '0', '2'))
    except Exception as e:
        results_list.append(f'Error: {e}')

    return results_list

execute_js_test = local_cloud.register(shredder_example, 'shredder_example')
res = execute_js_test().get()
for r in res:
    print(r)

