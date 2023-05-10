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

HOT_RATIO = 0.00001

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
        
        cloudburst_client.put_object(gen_userid(cur), list_slice, client_name=client_name)

        cloudburst_client.put_object(SHREDDER_TEST_KEY, list(range(0, 10)), client_name="shredder") # for test purpose
            
    logging.info('Finished generating dataset')

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    def read_single(cloudburst, key, client_name=DEFAULT_CLIENT_NAME):
        arr = cloudburst.get(gen_userid(key), client_name=client_name, raw=True)
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
        
        friends = cloudburst.get(gen_userid(id))
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
    
    if len(args) >= 3 and args[-1] == 'tput':
        return run_tput_example(cloudburst_client, num_requests, sckt, args)
    
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
    scheduler_time = []
    kvs_time = []

    retries = 0

    log_start = time.time()

    log_epoch = 0
    epoch_total = []

    request_count = 0

    for i in range(num_requests):
        request_count %= 20 # 1 read_single, 19 k_hop
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
        res = cloudburst_client.call_dag(dag_name, arg_map)
        end = time.time()
        s_time = end - start
        
        start = time.time()
        r = res.get()
        end = time.time()
        k_time = end - start
        
        scheduler_time += [s_time]
        kvs_time += [k_time]
        epoch_total += [s_time + k_time]
        total_time += [s_time + k_time]

        print(f"request {i} done, res {r}")

        log_end = time.time()
        if (log_end - log_start) > 10:
            if sckt:
                sckt.send(cp.dumps(epoch_total))
            utils.print_latency_stats(epoch_total, 'EPOCH %d E2E' %
                                        (log_epoch), True, bname='social_network', args=args, csv_filename='benchmark_lat.csv')

            epoch_total.clear()
            log_epoch += 1
            log_start = time.time()

    return total_time, scheduler_time, kvs_time, retries

def client_call_dag(cloudburst_client, stop_event, meta_dict, q, *args):
    userid_list, hot_userid_list, k, client_name = args
    if client_name == "hybrid":
        k_hop_client_name = "shredder"
        read_single_client_name = "anna"
    else:
        k_hop_client_name = client_name
        read_single_client_name = client_name
    logging.info(f'dag_name: {read_single_dag_name} and {k_hop_dag_name}, k: {k}, client_name: {client_name}')
    request_count = 0
    while True:
        if stop_event.is_set():
            break
        c_id = q.get()
        request_count %= 20 # 1 read_single, 19 k_hop
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
        
        meta_dict[c_id].reset()
        cloudburst_client.call_dag(dag_name, arg_map, direct_response=True, async_response=True, output_key=c_id)

def run_tput_example(cloudburst_client, num_clients, sckt, args):
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

    userid_list = cloudburst_client.get_object(key_args())
    hot_userid_list = random.choices(userid_list, k=int(len(userid_list)*HOT_RATIO))
    
    logging.info(f'Running social network, kvs_name {client_name}, k {k}')
    client_meta_dict = {}
    profiler = utils.Profiler(bname='social_network', num_clients=num_clients, args=args)

    client_q = queue.Queue(maxsize=num_clients)
    for i in range(num_clients):
        c_id = utils.gen_c_id(i)
        client_q.put(c_id)
        client_meta_dict[c_id] = utils.ClientMeta(c_id)
    
    stop_event = threading.Event()
    call_worker = threading.Thread(target=client_call_dag, args=(cloudburst_client, stop_event, client_meta_dict, client_q, userid_list, hot_userid_list, k, client_name), daemon=True)
    recv_worker = threading.Thread(target=utils.client_recv_dag_response, args=(cloudburst_client, stop_event, client_meta_dict, client_q, profiler), daemon=True)
    
    call_worker.start()
    recv_worker.start()
    
    for i in range(5):
        time.sleep(10)
        epoch_tput, epoch_lat = profiler.print_tput(csv_filename='benchmark_tput.csv')
        # send epoch result to trigger
        sckt.send(cp.dumps((epoch_tput, epoch_lat)))

    stop_event.set()
    call_worker.join()
    recv_worker.join()
    
    return profiler.lat, [], [], 0
