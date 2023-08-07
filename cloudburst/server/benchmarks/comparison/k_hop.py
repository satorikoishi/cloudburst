import logging
import random
import sys
import time

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.utils import DEFAULT_CLIENT_NAME

# Args: k
dag_name = 'k_hop'
rpc_fun_name = 'count_friend_list'

UPPER_BOUND = 1000

# def gen_userid(id):
#     return f'{dag_name}{id}'

# TODO: use real-world dataset
# def generate_dataset(cloudburst_client):
#     # 1000 users, randomly follow 0 - 100 users
#     all_users = np.arange(1000)
    
#     for i in range(1000):
#         follow_count = random.randrange(1, 100)
#         userid = gen_userid(i)
#         follow_arr = np.random.choice(all_users, follow_count, replace=False)
#         cloudburst_client.put_object(userid, follow_arr)
        
#         # print(f'User {userid} following {follow_count} users, they are: {follow_arr}')
    
#     logging.info('Finished generating dataset')

def gen_userid(id):
    return f'{id}'

def key_args():
    return '1000001'

def generate_dataset(cloudburst_client, client_name):    
    splitter = np.arange(0, UPPER_BOUND).tolist()
    cloudburst_client.put_object(key_args(), splitter, client_name=client_name)
    
    for cur in splitter:
        next = cur + 10
        
        if next < UPPER_BOUND:
            # normal case
            list_slice = list(range(cur+1, next+1))
        else:
            # last slice
            list_slice = list(range(cur+1, UPPER_BOUND))+list(range(0, next%UPPER_BOUND+1))
        
        cloudburst_client.put_object(gen_userid(cur), np.array(list_slice), client_name=client_name)
            
    logging.info('Finished generating dataset')

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    def k_hop(cloudburst, id, k, client_name=DEFAULT_CLIENT_NAME):
        if client_name == "shredder":
            sum = cloudburst.execute_js_fun(rpc_fun_name, id, k, client_name=client_name)
            return sum
        
        friends = cloudburst.get(gen_userid(id)).tolist()
        sum = len(friends)
        
        if k == 1:
            return sum
        
        for friend_id in friends:
            sum += k_hop(cloudburst, friend_id, k - 1)
        
        return sum


    cloud_k_hop = cloudburst_client.register(k_hop, dag_name)
    if cloud_k_hop:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, dag_name)
    logging.info('Finished registering dag')
    

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 2 and args[0] != 'create':
        print(f"{args} too short. Args: kvs_name, k")
        sys.exit(1)

    if args[0] == 'create':
        # Create dataset and DAG
        generate_dataset(cloudburst_client, DEFAULT_CLIENT_NAME)
        utils.shredder_setup_data(cloudburst_client, 'list_traversal')
        create_dag(cloudburst_client)
        return [], [], [], 0
    
    client_name = args[0]
    k = int(args[1])
    userid = 0
    # userid_list = cloudburst_client.get_object(key_args())
    
    logging.info(f'Running k-hop , kvs_name {client_name}, k {k}')

    total_time = []
    epoch_req_count = 0
    epoch_latencies = []
    exec_epoch_latencies = []

    for _ in range(num_requests):
        
        # userid = random.choice(userid_list)
        arg_map = {dag_name: [userid, k, client_name]}
        
        start = time.time()
        res = cloudburst_client.call_dag(dag_name, arg_map, True, exec_latency=True)
        end = time.time()

        if not res:
            continue
        
        exec_lat, _ = res
        
        epoch_req_count += 1
        
        total_time += [end - start]
        epoch_latencies += [end - start]
        exec_epoch_latencies += [exec_lat]
        
    if sckt:
        sckt.send(cp.dumps((epoch_req_count, epoch_latencies, exec_epoch_latencies)))
    
    return total_time, [], [], 0
