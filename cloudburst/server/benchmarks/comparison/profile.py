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

dag_name = 'profile'

KEY_MAX = 128000
HOT_KEY  = '0'
VALUE = 1

def generate_dataset(cloudburst_client, client_name):
    for key in range(KEY_MAX):
        cloudburst_client.put_object(str(key), VALUE, client_name=client_name)
        
        # debug logging
        if key % 1000 == 0:
            logging.info(f'Generating key: {key} for client: {client_name}')
    
    logging.info(f'Finished generating dataset for client {client_name}')

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    ## Access #access_count keys in a row, starts from #start_key
    ## Hot: access same key
    def profile(cloudburst, start_key, access_count, hot = False, client_name=DEFAULT_CLIENT_NAME):
        if hot:
            for _ in range(access_count):
                value = cloudburst.get(str(start_key), client_name=client_name)
            return value
        else:
            for key in range(int(start_key), int(start_key) + access_count):
                value = cloudburst.get(str(key), client_name=client_name)
            return value

    cloud_profile = cloudburst_client.register(profile, dag_name)
    if cloud_profile:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, dag_name)
    logging.info('Finished registering dag')
    

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 3 and args[0] != 'create':
        print(f"{args} too short. Args: kvs_name, hot, start_key, access_count")
        sys.exit(1)
        
    if args[0] == 'create':
        # Create dataset and DAG
        generate_dataset(cloudburst_client, DEFAULT_CLIENT_NAME)
        generate_dataset(cloudburst_client, 'shredder')
        create_dag(cloudburst_client)
        return [], [], [], 0
    else:
        client_name = args[0]
        start_key = args[2]
        if args[1] == '0':
            hot = False
        else:
            hot = True
            start_key = HOT_KEY
        access_count = int(args[3])
    
    logging.info(f'Running profile, kvs_name {client_name}')

    total_time = []
    epoch_req_count = 0
    epoch_latencies = []

    epoch_start = time.time()
    epoch = 0
    
    # TODO: get executor profile...
    for _ in range(num_requests):
        arg_map = {dag_name: [start_key, access_count, hot, client_name]}
        
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
                                        (epoch), True, bname='profile', args=args, csv_filename='benchmark_lat.csv')
            epoch += 1
            
            epoch_req_count = 0
            epoch_latencies.clear()
            epoch_start = time.time()
        
        if not hot:
            # Update start key for every request, to disable cache
            start_key = str(int(start_key) + access_count)
    
    logging.info(f'start key: {start_key}, hot: {hot}, access count: {access_count}, num request: {num_requests}')

    return total_time, [], [], 0
