import logging
import random
import sys
import time

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.utils import DEFAULT_CLIENT_NAME

dag_name = 'no_op'

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    def no_op(_):
        return 0xDEADBEEF

    cloud_no_op = cloudburst_client.register(no_op, dag_name)
    if cloud_no_op:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, dag_name)    

def run(cloudburst_client, num_requests, sckt, args):
    if args[0] == 'create':
        create_dag(cloudburst_client)
        return [], [], [], 0
    
    logging.info(f'Running {dag_name}, num_requests: {num_requests}')

    total_time = []
    epoch_req_count = 0
    epoch_latencies = []
    exec_epoch_latencies = []

    epoch_start = time.time()
    epoch = 0

    for _ in range(num_requests):
        arg_map = {dag_name: []}
        
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

        epoch_end = time.time()
        if (epoch_end - epoch_start) > 10:
            if sckt:
                sckt.send(cp.dumps((epoch_req_count, epoch_latencies, exec_epoch_latencies)))
            utils.print_latency_stats(epoch_latencies, 'EPOCH %d E2E' %
                                        (epoch), True, bname=f'{dag_name}', args=args, csv_filename='benchmark_lat.csv')

            epoch += 1
            
            epoch_req_count = 0
            epoch_latencies.clear()
            epoch_start = time.time()

    return total_time, [], [], 0
