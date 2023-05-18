import logging
import random
import sys
import time
import queue
import threading

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.utils import DEFAULT_CLIENT_NAME

read_single_dag_name = 'read_single'
k_hop_dag_name = 'k_hop'
rpc_fun_name = 'count_friend_list'

# 10000 lists, out of 100000 numbers
UPPER_BOUND = 100000

HOT_RATIO = 0.0001

# for test purpose: make sure the payload size is the same as in the anna version
SHREDDER_TEST_KEY = '1000002'
TEST_SHREDDER_LATTICE = True

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

        cloudburst_client.put_object(SHREDDER_TEST_KEY, np.array(list(range(0, 10))), client_name="shredder") # for test purpose
            
    logging.info('Finished generating dataset')

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    def read_single(cloudburst, key, client_name=DEFAULT_CLIENT_NAME):
        arr = cloudburst.get(gen_userid(key), client_name=client_name)
        return arr

    cloud_read_single = cloudburst_client.register(read_single, read_single_dag_name)
    if cloud_read_single:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)

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


    cloud_k_hop = cloudburst_client.register(k_hop, k_hop_dag_name)
    if cloud_k_hop:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, "read_single")
    utils.register_dag_for_single_func(cloudburst_client, "k_hop")
    logging.info('Finished registering dag')
    

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 2 and args[0] != 'create':
        print(f"{args} too short. Args: kvs_name, k")
        sys.exit(1)

    if args[0] == 'create':
        # Create dataset and DAG
        generate_dataset(cloudburst_client, DEFAULT_CLIENT_NAME)
        utils.shredder_setup_data(cloudburst_client)
        create_dag(cloudburst_client)
        return [], [], [], 0
    
    client_name = args[0]
    k = int(args[1])

    if client_name == "hybrid":
        k_hop_client_name = "shredder"
        read_single_client_name = "anna"
    else:
        k_hop_client_name = client_name
        read_single_client_name = client_name

    userid_list = cloudburst_client.get_object(key_args())
    hot_userid_list = random.choices(userid_list, k=int(len(userid_list)*HOT_RATIO))
    
    logging.info(f'Running social network, kvs_name {client_name}, k {k}')

    total_time = []
    epoch_req_count = 0
    epoch_latencies = []

    epoch_start = time.time()
    epoch = 0

    request_count = 0
    for i in range(num_requests):
        request_count %= 20 # 1 k_hop, 19 read_single
        if request_count == 0:
            dag_name = k_hop_dag_name
            userid = random.choice(userid_list)
            arg_map = {dag_name: [userid, k, k_hop_client_name]}
        else:
            dag_name = read_single_dag_name
            if random.random() < 0.05: # 95% of reads are hot
                userid = random.choice(userid_list)
            else:
                userid = random.choice(hot_userid_list)

            if TEST_SHREDDER_LATTICE and read_single_client_name == "shredder":
                userid = SHREDDER_TEST_KEY  # for test purpose

            arg_map = {dag_name: [userid, read_single_client_name]}
        request_count += 1
        
        start = time.time()
        res = cloudburst_client.call_dag(dag_name, arg_map, True)
        end = time.time()

        if res is not None:
            epoch_req_count += 1

        total_time += [end - start]
        epoch_latencies += [end - start]

        epoch_end = time.time()
        if (epoch_end - epoch_start) > 10:
            if sckt:
                sckt.send(cp.dumps((epoch_req_count, epoch_latencies)))
            utils.print_latency_stats(epoch_latencies, 'EPOCH %d E2E' %
                                        (epoch), True, bname='social_network', args=args, csv_filename='benchmark_lat.csv')
            epoch += 1

            epoch_req_count = 0
            epoch_latencies.clear()
            epoch_start = time.time()

    return total_time, [], [], 0

