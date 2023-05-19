import logging
import random
import sys
import time

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.utils import DEFAULT_CLIENT_NAME

dag_name = 'accumulate'
rpc_fun_name = 'accumulate'

KEY = '10000010'
VALUE = 1
REPEAT_NUM = 100

def generate_dataset(cloudburst_client, client_name):    
    cloudburst_client.put_object(KEY, VALUE, client_name=client_name)
    logging.info('Finished generating dataset')

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    def accumulate(cloudburst, k, client_name=DEFAULT_CLIENT_NAME):
        sum = 0
        
        for _ in range (REPEAT_NUM):
            v = cloudburst.get(k, client_name=client_name)
            sum += v

        return sum

    cloud_accumulate = cloudburst_client.register(accumulate, dag_name)
    if cloud_accumulate:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, dag_name)
    logging.info('Finished registering dag')
    

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 1 and args[0] != 'create':
        print(f"{args} too short. Args: kvs_name")
        sys.exit(1)

    if args[0] == 'create':
        # Create dataset and DAG
        generate_dataset(cloudburst_client, DEFAULT_CLIENT_NAME)
        generate_dataset(cloudburst_client, 'shredder')
        utils.shredder_setup_data(cloudburst_client)
        create_dag(cloudburst_client)
        return [], [], [], 0
    
    client_name = args[0]
    
    logging.info(f'Running accumulate , kvs_name {client_name}')

    total_time = []
    epoch_req_count = 0
    epoch_latencies = []

    epoch_start = time.time()
    epoch = 0

    for _ in range(num_requests):
        
        key = KEY
        arg_map = {dag_name: [key, client_name]}
        
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
                                        (epoch), True, bname='accumulate', args=args, csv_filename='benchmark_lat.csv')
            epoch += 1

            epoch_req_count = 0
            epoch_latencies.clear()
            epoch_start = time.time()

    return total_time, [], [], 0
