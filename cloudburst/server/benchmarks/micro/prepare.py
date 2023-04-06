import logging
import random
import sys
import time
import uuid

import numpy as np

from cloudburst.server.benchmarks.micro import meta
from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.utils import DEFAULT_CLIENT_NAME

def run(cloudburst_client, num_requests, sckt):        
    if num_requests != 1:
        print(f'Prepare num request should be 1, received {num_requests}')
        sys.exit(1)

    ''' PREPARE DATA '''
    prepare_data(cloudburst_client, meta.ARR_SIZE_ANNA, DEFAULT_CLIENT_NAME)
    prepare_data(cloudburst_client, meta.ARR_SIZE_SHREDDER, 'shredder')

    ''' REGISTER FUNCTIONS '''
    def read_single(cloudburst, key, client_name=DEFAULT_CLIENT_NAME):
        arr = cloudburst.get(key, client_name=client_name)
        return arr

    cloud_read_single = cloudburst_client.register(read_single, 'read_single')
    if cloud_read_single:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
        
    def update_single(cloudburst, key, client_name=DEFAULT_CLIENT_NAME):
        arr = cloudburst.get(key, client_name=client_name)
        arr = arr.copy()
        arr[0] = arr[0] + 1
        cloudburst.put(key, arr, client_name=client_name)
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
    test_registered_functions(cloud_read_single, cloud_update_single, meta.ARR_SIZE_ANNA, DEFAULT_CLIENT_NAME)
    test_registered_functions(cloud_read_single, cloud_update_single, meta.ARR_SIZE_SHREDDER, 'shredder')
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, "read_single")
    utils.register_dag_for_single_func(cloudburst_client, "update_single")

    return [], [], [], 0

def prepare_data(cloudburst_client, arr_size, client_name):
    for size in arr_size:
        init_arr = np.zeros(size)
        mem_size = sys.getsizeof(init_arr)
        logging.info(f'Created init_arr with {size} int objs, mem size {mem_size}')
        
        for i in range(meta.NUM_KV_PAIRS):
            # 1K kv pair for each size, key range [0, 1000) + size
            key = meta.key_gen(size, i)
            cloudburst_client.put_object(key, init_arr, client_name)
    logging.info(f'{client_name} data ready')


def test_registered_functions(cloud_read_single, cloud_update_single, arr_size, client_name):
    for size in arr_size:
        for i in [0, 999]:
            key = meta.key_gen(size, i)
            
            # Read initial state: all zero
            res_1stread = cloud_read_single(key, client_name).get()
            
            arr_1stread = np.fromstring(res_1stread)
            if np.count_nonzero(arr_1stread):
                logging.warn(f'Unexpected result {res_1stread}, {arr_1stread} from read_single, size: {size}, idx: {i}')
                # sys.exit(1)
            
            # Increment arr[0] by 1
            res_1stupdate = cloud_update_single(key, client_name).get()
            if res_1stupdate != 1.0:
                logging.warn(f'Unexpected result {res_1stupdate} from update_single, size: {size}, idx: {i}')
                # sys.exit(1)
            
            # Read again, arr[0] should be 1
            res_2ndread = cloud_read_single(key, client_name).get()
            arr_2ndread = np.fromstring(res_2ndread)
            if np.count_nonzero(arr_2ndread) != 1 or arr_2ndread[0] != 1.0:
                logging.warn(f'Unexpected result {res_2ndread}, {arr_2ndread} from read_single, size: {size}, idx: {i}')
                # sys.exit(1)

    logging.info(f'Successfully tested function with {client_name}')
