import logging
import random
import sys
import time

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.utils import DEFAULT_CLIENT_NAME

dag_name = 'compute_emulate'
KEY_MAX = 10
VALUE = '1'
SEC_TO_USEC = 1000000.0

def generate_dataset(cloudburst_client, client_name):
    for key in range(KEY_MAX):
        cloudburst_client.put_object(str(key), VALUE, client_name=client_name)

    logging.info(f'Finished generating dataset for client {client_name}')

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    def compute_emulate(cloudburst, key, access_count, compute_duration, client_name=DEFAULT_CLIENT_NAME):
        if client_name == "shredder":
            value = cloudburst.execute_js_fun(dag_name, key, access_count, compute_duration, client_name=client_name)
        else:
            # precised sleep
            now = time.time()
            end = now + compute_duration / SEC_TO_USEC
            while now < end:
                now = time.time()
            
            for i in range(access_count):
                value = cloudburst.get(str(key), client_name=client_name)
        return value

    cloud_compute_emulate = cloudburst_client.register(compute_emulate, dag_name)
    if cloud_compute_emulate:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, dag_name)    

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 3 and args[0] != 'create':
        print(f"{args} too short. Args: client_name, access_count, duration")
        sys.exit(1)
        
    if args[0] == 'create':
        generate_dataset(cloudburst_client, DEFAULT_CLIENT_NAME)
        utils.shredder_setup_data(cloudburst_client, f'{dag_name}_setup')
        create_dag(cloudburst_client)
        return [], [], [], 0
    
    logging.info(f'Running {dag_name}, num_requests: {num_requests}')
    
    key = '1'
    client_name = args[0]
    access_count = int(args[1])
    compute_duration = int(args[2]) # us

    total_time = []
    epoch_req_count = 0
    epoch_latencies = []
    exec_epoch_latencies = []

    for _ in range(num_requests):
        arg_map = {dag_name: [key, access_count, compute_duration, client_name]}
        
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
