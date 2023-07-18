import logging
import random
import sys
import time

import cloudpickle as cp
import numpy as np
import os

from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.utils import DEFAULT_CLIENT_NAME

experiment_name = 'facebook_social'
heavy_dag_name = 'facebook_list_traversal'
light_dag_name = 'facebook_get'
dataset_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '../dataset/facebook_combined.txt')
KEY = '0'     ## TODO: fix this

def generate_dataset(cloudburst_client, client_name):
    cur_k = '0'
    cur_v = []
    with open(dataset_path, 'r') as f:
        for line in f.readlines():
            k, v = line.split()
            if int(k) > int(cur_k):
                cloudburst_client.put_object(cur_k, cur_v, client_name=client_name)
                cur_k = k
                cur_v = []
            elif int(k) == int(cur_k):
                cur_v.append(v)
            else:
                assert False

    logging.info(f'Finished generating dataset for client {client_name}')

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    def facebook_get(cloudburst, key, client_name=DEFAULT_CLIENT_NAME):
        value = None
        if client_name == "shredder":
            value = cloudburst.execute_js_fun(light_dag_name, key, client_name=client_name)
        else:
            value = cloudburst.get(str(key), client_name=client_name)
        return value

    cloud_facebook_get = cloudburst_client.register(facebook_get, light_dag_name)
    if cloud_facebook_get:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
        
    def facebook_list_traversal(cloudburst, key, depth, client_name=DEFAULT_CLIENT_NAME):
        if client_name == "shredder":
            value = cloudburst.execute_js_fun(heavy_dag_name, key, client_name=client_name)
        else:
            for _ in range(depth):
                value = cloudburst.get(str(key), client_name=client_name)
                key = value[0]
        return value

    cloud_facebook_list_traversal = cloudburst_client.register(facebook_list_traversal, heavy_dag_name)
    if cloud_facebook_list_traversal:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, light_dag_name)    
    utils.register_dag_for_single_func(cloudburst_client, heavy_dag_name)    

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 2 and args[0] != 'create':
        print(f"{args} too short. Args: client_name, percent, depth")
        sys.exit(1)
        
    if args[0] == 'create':
        generate_dataset(cloudburst_client, DEFAULT_CLIENT_NAME)
        utils.shredder_setup_data(cloudburst_client, f'{experiment_name}_setup')
        create_dag(cloudburst_client)
        return [], [], [], 0
        
    key = KEY
    client_name = args[0]
    heavy_percent = int(args[1])
    depth = int(args[2])
    
    logging.info(f'Running {experiment_name}, num_requests: {num_requests}, heavy_percent: {heavy_percent}, depth: {depth}')

    total_time = []
    epoch_req_count = 0
    epoch_latencies = []
    exec_epoch_latencies = []

    for _ in range(num_requests):
        choice = random.randrange(100)
        
        start = time.time()
        if choice < heavy_percent:
            # Heavy DAG
            arg_map = {heavy_dag_name: [key, depth, client_name]}
            res = cloudburst_client.call_dag(heavy_dag_name, arg_map, True, exec_latency=True)
        else:
            # Light DAG
            arg_map = {light_dag_name: [key, client_name]}
            res = cloudburst_client.call_dag(light_dag_name, arg_map, True, exec_latency=True)
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
