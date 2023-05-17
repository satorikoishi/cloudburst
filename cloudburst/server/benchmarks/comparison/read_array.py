import logging
import random
import sys
import time
import queue
import threading

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.utils import DEFAULT_CLIENT_NAME

dag_name = 'read_array'

# 100000 lists, out of 100000 numbers
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
    def read_array(cloudburst, key, client_name=DEFAULT_CLIENT_NAME):
        arr = cloudburst.get(gen_userid(key), client_name=client_name)
        return arr

    cloud_read_array = cloudburst_client.register(read_array, dag_name)
    if cloud_read_array:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, dag_name)
    logging.info('Finished registering dag')
    

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 2 and args[0] != 'create':
        print(f"{args} too short. Args: kvs_name, userid")
        sys.exit(1)

    if args[0] == 'create':
        # Create dataset and DAG
        generate_dataset(cloudburst_client, DEFAULT_CLIENT_NAME)
        utils.shredder_setup_data(cloudburst_client)
        create_dag(cloudburst_client)
        return [], [], [], 0
    
    client_name = args[0]
    userid_list = cloudburst_client.get_object(key_args())
    hot_userid_list = random.choices(userid_list, k=int(len(userid_list)*HOT_RATIO))
    
    logging.info(f'Running read array, kvs_name {client_name}')

    total_time = []
    epoch_req_count = 0
    epoch_latencies = []

    epoch_start = time.time()
    epoch = 0

    for _ in range(num_requests):
        if random.random() < 0.05: # 95% of reads are hot
            userid = random.choice(userid_list)
        else:
            userid = random.choice(hot_userid_list)

        if TEST_SHREDDER_LATTICE and client_name == "shredder":
            userid = SHREDDER_TEST_KEY  # for test purpose

        arg_map = {dag_name: [userid, client_name]}
        
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
                                        (epoch), True, bname='read_array', args=args, csv_filename='benchmark_lat.csv')

            epoch_req_count = 0
            epoch_latencies.clear()
            epoch_start = time.time()

    return total_time, [], [], 0
