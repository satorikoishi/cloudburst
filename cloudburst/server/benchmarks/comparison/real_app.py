import logging
import random
import sys
import time
import os
import base64
import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.utils import DEFAULT_CLIENT_NAME

dag_name = 'real_app'
real_app_list = ['auth', 'calc_avg', 'k_hop', 'file_replicator', 'list_traversal', 'user_follow', 'image', 'video', 'ml_serving']
KEY_MAX = 1000
VALUE = 'a' * 1024

def generate_dataset(cloudburst_client, app_name, client_name):
    if app_name in ['auth']:
        for key in range(KEY_MAX):
            cloudburst_client.put_object(str(key), VALUE, client_name=client_name)
    elif app_name in ['k_hop', 'list_traversal', 'list_traversal', 'user_follow']:
        dataset_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '../dataset/facebook_combined.txt')
        edge_0 = []
        with open(dataset_path, 'r') as file:
            for line in file:
                parts = line.strip().split()
                first= parts[0]
                if int(first) > 0:
                    break
                second = parts[1]
                if int(first) == 0:
                    edge_0.append(second)
            concatenated_string = ' '.join(x for x in edge_0)
            for key in range(KEY_MAX):
                cloudburst_client.put_object(key, concatenated_string)
    elif app_name in ['calc_avg']:
        for i in range(KEY_MAX):
            random_ints = [random.randint(0, 10000) for _ in range(10)]
            concatenated_string = ' '.join(str(num) for num in random_ints)
            cloudburst_client.put_object(str(i), concatenated_string, client_name=client_name)
    elif app_name in ['file_replicator']:
        FILE_SIZE = 1024 * 1024
        FILE_NAME = 'tmp.dat'
        with open(FILE_NAME, 'wb') as f:
            # os.urandom generates random bytes
            f.write(os.urandom(FILE_SIZE))
        with open(FILE_NAME, 'rb') as f:
            data = f.read()
        cloudburst_client.put_object('source', base64.b64encode(data), client_name=client_name)
        
    logging.info(f'Finished generating dataset for client {client_name}')

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    def app_collection(cloudburst, key, app_name, client_name=DEFAULT_CLIENT_NAME):
        res = None
        if client_name == 'pocket':
            utils.emulate_exec(utils.POCKET_INIT_LATENCY)
            
        if app_name == 'auth':
            if client_name == 'pocket':
                utils.emulate_exec(utils.POCKET_MOCK_LATENCY)
            else:
                res = cloudburst.get(key, client_name=client_name)
        elif app_name == 'calc_avg':
            if client_name == 'pocket':
                utils.emulate_exec(utils.POCKET_MOCK_LATENCY)
                random_ints = [random.randint(0, 10000) for _ in range(10)]
                concatenated_string = ' '.join(str(num) for num in random_ints)
            else:
                concatenated_string = cloudburst.get(key, client_name=client_name)
            extracted_ints = [int(num) for num in concatenated_string.split()]
            res = sum(extracted_ints) / len(extracted_ints)
        elif app_name == 'file_replicator':
            if client_name == 'pocket':
                utils.emulate_exec(utils.POCKET_1M_LATENCY)
                utils.emulate_exec(utils.POCKET_1M_LATENCY)
            else:
                res = cloudburst.get('source', client_name=client_name)
                res = cloudburst.put('target', res, client_name=client_name)
        elif app_name == 'user_follow':
            if client_name == 'pocket':
                utils.emulate_exec(utils.POCKET_MOCK_LATENCY)
                utils.emulate_exec(utils.POCKET_MOCK_LATENCY)
                utils.emulate_exec(utils.POCKET_MOCK_LATENCY)
            else:
                key = '0'
                concatenated_string = cloudburst.get(key, client_name=client_name)
                res = cloudburst.put(key, concatenated_string, client_name=client_name)
        elif app_name == 'k_hop':
            # k = 2
            key = '0'
            access_count = 1
            concatenated_string = cloudburst.get(key, client_name='anna')
            direct_friends = concatenated_string.split()
            for _ in direct_friends:
                concatenated_string = cloudburst.get(key, client_name='anna')
                access_count += 1
            if client_name == 'pocket':
                utils.emulate_exec(utils.POCKET_MOCK_LATENCY - 210)
        elif app_name == 'list_traversal':
            key = '0'
            for _ in range(8):
                if client_name == 'pocket':
                    utils.emulate_exec(utils.POCKET_MOCK_LATENCY)
                else:
                    cloudburst.get(key, client_name=client_name)
        elif app_name == 'list_traversal_mix':
            depth = int(key)
            key = '0'
            for _ in range(depth):
                if client_name == 'pocket':
                    utils.emulate_exec(3 * utils.POCKET_MOCK_LATENCY)
                else:
                    cloudburst.get(key, client_name=client_name)
        else:
            assert False
            
        return res

    ret = cloudburst_client.register(app_collection, dag_name)
    if ret:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, dag_name)    

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 2 and args[0] != 'create':
        print(f"{args} too short. Args: client_name, app_name")
        sys.exit(1)
        
    if args[0] == 'create':
        app_name = args[1]
        generate_dataset(cloudburst_client, app_name, DEFAULT_CLIENT_NAME)
        create_dag(cloudburst_client)
        return [], [], [], 0
    
    while True:
        key = np.random.zipf(2) - 1
        if key < KEY_MAX:
            break
    key = str(key)
    client_name = args[0]
    app_name = args[1]
    
    logging.info(f'Running {app_name}, num_requests: {num_requests}')

    total_time = []
    epoch_req_count = 0
    epoch_latencies = []
    exec_epoch_latencies = []

    for i in range(num_requests):
        if app_name in ['k_hop', 'list_traversal', 'user_follow']:
            key = str(i % KEY_MAX)
        if app_name == 'list_traversal_mix':
            r = random.random()
            if r < 0.4:
                key = 1
            elif r < 0.6:
                key = 2
            elif r < 0.8:
                key = 4
            else:
                key = 8
        arg_map = {dag_name: [key, app_name, client_name]}
        
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
