import logging
import random
import sys
import time

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.utils import DEFAULT_CLIENT_NAME

dag_name = 'data_size'
KEY_MAX = 1000

def generate_dataset(cloudburst_client, client_name, data_size):
    VALUE = 'a' * data_size
    for key in range(KEY_MAX):
        cloudburst_client.put_object(str(key), VALUE, client_name=client_name)

    logging.info(f'Finished generating dataset for client {client_name}')

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    def data_size_test(cloudburst, key, access_count, compute_duration, client_name=DEFAULT_CLIENT_NAME):
        value = None
        if client_name == "shredder":
            value = cloudburst.execute_js_fun(dag_name, key, access_count, compute_duration, client_name=client_name)
        else:
            if client_name == 'pocket':
                utils.emulate_exec(utils.POCKET_INIT_LATENCY)
            utils.emulate_exec(compute_duration)
            
            for i in range(access_count):
                if client_name == 'pocket':
                    value = 0
                    utils.emulate_exec(utils.POCKET_MOCK_LATENCY)
                else:
                    value = cloudburst.get(str(int(key) + i), client_name=client_name)
        return value

    cloud_data_size_test = cloudburst_client.register(data_size_test, dag_name)
    if cloud_data_size_test:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, dag_name)    

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 4 and args[0] != 'create':
        print(f"{args} too short. Args: client_name, access_count, data_size, duration")
        sys.exit(1)
        
    if args[0] == 'create':
        data_size = int(args[1])
        generate_dataset(cloudburst_client, DEFAULT_CLIENT_NAME, data_size)
        create_dag(cloudburst_client)
        return [], [], [], 0
    
    logging.info(f'Running {dag_name}, num_requests: {num_requests}')
    
    key = '1'
    cold_flag = False
    if args[0].isnumeric():
        # Test cold
        start_key = args[0]
        client_name = 'anna'
        cold_flag = True
    else:
        # Normal case
        client_name = args[0]
    access_count = int(args[1])
    data_size = int(args[2]) # useless, just for results
    compute_duration = int(args[3]) # us

    total_time = []
    epoch_req_count = 0
    epoch_latencies = []
    exec_epoch_latencies = []

    for i in range(num_requests):
        if cold_flag:
            key = str(int(start_key) + i * access_count)
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
