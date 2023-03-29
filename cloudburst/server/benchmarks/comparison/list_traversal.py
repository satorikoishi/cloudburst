import logging
import random
import sys
import time

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference

# Args: k
dag_name = 'list_traversal'

# 1000 lists, out of 10000 numbers
UPPER_BOUND = 10000

# def gen_nodeid(id):
#     return f'{dag_name}{id}'

# def key_args():
#     return f'{dag_name}_args'

# def generate_dataset(cloudburst_client):    
#     splitter = np.sort(np.random.choice(np.arange(1, UPPER_BOUND, dtype=int), 999, replace=False)).tolist()
#     splitter = [0] + splitter
#     cloudburst_client.put_object(key_args(), splitter)
#     splitter = splitter + [0]
    
#     for offset in range(1000):
#         cur = splitter[offset]
#         next = splitter[offset + 1]
        
#         if next != 0:
#             # normal case
#             list_slice = list(range(cur, next))
#         else:
#             # last slice
#             list_slice = list(range(cur, UPPER_BOUND + 1))
        
#         # First element points to next list
#         list_slice[0] = next
        
#         cloudburst_client.put_object(gen_nodeid(cur), list_slice)
            
#     logging.info('Finished generating dataset')

def gen_nodeid(id):
    return f'{id}'

def key_args():
    return '100000'

def generate_dataset(cloudburst_client):    
    splitter = np.arange(0, UPPER_BOUND, 10).tolist()
    cloudburst_client.put_object(key_args(), splitter)
    splitter = splitter + [0]
    
    for offset in range(1000):
        cur = splitter[offset]
        next = splitter[offset + 1]
        
        if next != 0:
            # normal case
            list_slice = list(range(cur, next))
        else:
            # last slice
            list_slice = list(range(cur, UPPER_BOUND))
        
        # First element points to next list
        list_slice[0] = next
        
        cloudburst_client.put_object(gen_nodeid(cur), list_slice)
            
    logging.info('Finished generating dataset')

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    def list_traversal(cloudburst, nodeid, depth):
        for i in range(depth):
            nodeid = cloudburst.get(gen_nodeid(nodeid))[0]
        return nodeid

    cloud_list_traversal = cloudburst_client.register(list_traversal, dag_name)
    if cloud_list_traversal:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, dag_name)
    logging.info('Finished registering dag')
    

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 1:
        print(f"{args} too short. Args: depth")
        sys.exit(1)
    
    if args[0] == 'c':
        # Create dataset and DAG
        generate_dataset(cloudburst_client)
        create_dag(cloudburst_client)
        return [], [], [], 0
    
    depth = int(args[0])
    nodeid_list = cloudburst_client.get_object(key_args())
    
    logging.info(f'Running list traversal, depth {depth}')

    total_time = []
    scheduler_time = []
    kvs_time = []

    retries = 0

    log_start = time.time()

    log_epoch = 0
    epoch_total = []

    for i in range(num_requests):
        nodeid = random.choice(nodeid_list)
        # DAG name = Function name
        arg_map = {dag_name: [nodeid, depth]}
        
        start = time.time()
        res = cloudburst_client.call_dag(dag_name, arg_map)
        end = time.time()
        s_time = end - start
        
        start = time.time()
        res.get()
        end = time.time()
        k_time = end - start
        
        scheduler_time += [s_time]
        kvs_time += [k_time]
        epoch_total += [s_time + k_time]
        total_time += [s_time + k_time]

        log_end = time.time()
        if (log_end - log_start) > 10:
            if sckt:
                sckt.send(cp.dumps(epoch_total))
            utils.print_latency_stats(epoch_total, 'EPOCH %d E2E' %
                                        (log_epoch), True, bname='list_traversal', args=args, csv_filename='benchmark.csv')

            epoch_total.clear()
            log_epoch += 1
            log_start = time.time()

    return total_time, scheduler_time, kvs_time, retries
