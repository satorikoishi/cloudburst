import logging
import random
import sys
import time
import uuid

import numpy as np

from cloudburst.server.benchmarks.micro import meta
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.proto.cloudburst_pb2 import CloudburstError, DAG_ALREADY_EXISTS

def register_dag_for_single_func(cloudburst_client, function_name):
    functions = [function_name]
    connections = []
    success, error = cloudburst_client.register_dag(function_name, functions,
                                                     connections)
    if not success and error != DAG_ALREADY_EXISTS:
        print('Failed to register DAG: %s' % (CloudburstError.Name(error)))
        sys.exit(1)

def run(cloudburst_client, num_requests, sckt):        
    if num_requests != 1:
        print(f'Prepare num request should be 1, received {num_requests}')
        sys.exit(1)

    ''' PREPARE DATA '''
    for size in meta.ARR_SIZE:
        init_arr = np.zeros(size)
        mem_size = sys.getsizeof(init_arr)
        logging.info(f'Created init_arr with {size} int objs, mem size {mem_size}')
        
        for i in range(meta.NUM_KV_PAIRS):
            # 1K kv pair for each size, key range [0, 1000) + size
            key = meta.key_gen(size, i)
            cloudburst_client.put_object(key, init_arr)

    logging.info('Data ready')

    ''' REGISTER FUNCTIONS '''
    def read_single(cloudburst, key):
        arr = cloudburst.get(key)
        return arr

    cloud_read_single = cloudburst_client.register(read_single, 'read_single')
    if cloud_read_single:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
        
    def update_single(cloudburst, key):
        arr = cloudburst.get(key)
        arr = arr.copy()
        arr[0] = arr[0] + 1
        cloudburst.put(key, arr)
        return arr[0]

    cloud_update_single = cloudburst_client.register(update_single, 'update_single')
    if cloud_update_single:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)

    logging.info('Function ready')

    ''' TEST REGISTERED FUNCTIONS '''
    # Test may fail due to inconsistency
    for size in meta.ARR_SIZE:
        for i in [0, 999]:
            key = meta.key_gen(size, i)
            
            # Read initial state: all zero
            res_1stread = cloud_read_single(key).get()
            
            arr_1stread = np.fromstring(res_1stread)
            if np.count_nonzero(arr_1stread):
                logging.warn(f'Unexpected result {res_1stread}, {arr_1stread} from read_single, size: {size}, idx: {i}')
                # sys.exit(1)
            
            # Increment arr[0] by 1
            res_1stupdate = cloud_update_single(key).get()
            if res_1stupdate != 1.0:
                logging.warn(f'Unexpected result {res_1stupdate} from update_single, size: {size}, idx: {i}')
                # sys.exit(1)
            
            # Read again, arr[0] should be 1
            res_2ndread = cloud_read_single(key).get()
            arr_2ndread = np.fromstring(res_2ndread)
            if np.count_nonzero(arr_2ndread) != 1 or arr_2ndread[0] != 1.0:
                logging.warn(f'Unexpected result {res_2ndread}, {arr_2ndread} from read_single, size: {size}, idx: {i}')
                # sys.exit(1)

    logging.info('Successfully tested function!')
    
    ''' REGISTER DAG '''
    register_dag_for_single_func(cloudburst_client, "read_single")
    register_dag_for_single_func(cloudburst_client, "update_single")

    return [], [], [], 0
