import logging
import random
import sys
import time

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.utils import DEFAULT_CLIENT_NAME

dag_name = 'ycsb'
KEY_MAX = 1000
VALUE = 'a' * 1024

def generate_dataset(cloudburst_client, client_name):
    for key in range(KEY_MAX):
        cloudburst_client.put_object(str(key), VALUE, client_name=client_name)

    logging.info(f'Finished generating dataset for client {client_name}')

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    def ycsb(cloudburst, key, ycsb_type, client_name=DEFAULT_CLIENT_NAME):
        value = None
        if client_name == 'pocket':
            utils.emulate_exec(utils.POCKET_INIT_LATENCY)
            utils.emulate_exec(utils.POCKET_MOCK_LATENCY)
            if ycsb_type == 'F':
                if random.random() < 0.5:
                    utils.emulate_exec(utils.POCKET_MOCK_LATENCY)
            return value
        
        if ycsb_type == 'A':
            if random.random() < 0.5:
                cloudburst.put(key, VALUE, client_name=client_name)
            else:
                cloudburst.get(key, client_name=client_name)
        elif ycsb_type == 'B':
            if random.random() < 0.05:
                cloudburst.put(key, VALUE, client_name=client_name)
            else:
                cloudburst.get(key, client_name=client_name)
        elif ycsb_type == 'C':
            cloudburst.get(key, client_name=client_name)
        elif ycsb_type == 'D':
            if random.random() < 0.05:
                cloudburst.put(key, VALUE, client_name=client_name)
            else:
                cloudburst.get(key, client_name=client_name)
        elif ycsb_type == 'F':
            cloudburst.get(key, client_name=client_name)
            if random.random() < 0.5:
                cloudburst.put(key, VALUE, client_name=client_name)
        else:
            assert False
            
        return value

    ret = cloudburst_client.register(ycsb, dag_name)
    if ret:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, dag_name)    

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 2 and args[0] != 'create':
        print(f"{args} too short. Args: client_name, ycsb_type")
        sys.exit(1)
        
    if args[0] == 'create':
        generate_dataset(cloudburst_client, DEFAULT_CLIENT_NAME)
        create_dag(cloudburst_client)
        return [], [], [], 0
    
    logging.info(f'Running {dag_name}, num_requests: {num_requests}')
    
    while True:
        key = np.random.zipf(2) - 1
        if key < KEY_MAX:
            break
    key = str(key)
    client_name = args[0]
    ycsb_type = args[1] # us

    total_time = []
    epoch_req_count = 0
    epoch_latencies = []
    exec_epoch_latencies = []

    for i in range(num_requests):
        arg_map = {dag_name: [key, ycsb_type, client_name]}
        
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
