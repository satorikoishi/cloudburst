import logging
import random
import sys
import time

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.utils import DEFAULT_CLIENT_NAME

# Use compute emulate, but param varies
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
        value = None
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
    
    def compute_emulate_disable(cloudburst, key, access_count, compute_duration, client_name=DEFAULT_CLIENT_NAME):
        value = None
        if client_name == "shredder":
            value = cloudburst.execute_js_fun(dag_name, key, access_count, compute_duration, client_name=client_name)
        else:
            # precised sleep
            now = time.time()
            end = now + compute_duration / SEC_TO_USEC
            while now < end:
                now = time.time()
                
            # Disable Arbiter
            ac_alias = access_count
            
            for i in range(ac_alias):
                value = cloudburst.get(str(key), client_name=client_name)
        return value

    cloud_compute_emulate_disable = cloudburst_client.register(compute_emulate_disable, f'{dag_name}_disable')
    if cloud_compute_emulate_disable:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, dag_name)    
    utils.register_dag_for_single_func(cloudburst_client, f'{dag_name}_disable')    

def run(cloudburst_client, num_requests, sckt, args):
    global dag_name
    
    if len(args) < 1 and args[0] != 'create':
        print(f"{args} too short. Args: client_name")
        sys.exit(1)
        
    if args[0] == 'create':
        generate_dataset(cloudburst_client, DEFAULT_CLIENT_NAME)
        utils.shredder_setup_data(cloudburst_client, f'{dag_name}_setup')
        create_dag(cloudburst_client)
        return [], [], [], 0
    
    logging.info(f'Running {dag_name}, num_requests: {num_requests}')
    
    key = '1'
    client_name = args[0]
    compute_duration = 0
    if client_name == 'disable':
        client_name = 'arbiter'
        dag_name = f'{dag_name}_disable'

    total_time = []
    epoch_req_count = 0
    epoch_latencies = []
    exec_epoch_latencies = []

    for i in range(num_requests):
        interval = i // 100
        if interval % 2 == 0:
            access_count = 16
        else:
            access_count = 1
        
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
