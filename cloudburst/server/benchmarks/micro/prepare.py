import logging
import random
import sys
import time
import uuid

import numpy as np

from cloudburst.server.benchmarks.micro import meta
from cloudburst.shared.reference import CloudburstReference

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
        import cloudpickle as cp
        arr = cloudburst.get(key).reveal()
        return cp.loads(arr.tobytes())

    cloud_read_single = cloudburst_client.register(read_single, 'read_single')
    if cloud_read_single:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
        
    def update_single(cloudburst, key):
        arr = cloudburst.get(key).reveal()
        arr[0] = arr[0] + 1
        cloudburst.put(key, arr)
        
        arr_typename = type(arr).__name__
        a0_typename = type(arr[0]).__name__
        return f'arr: {arr} of type {arr_typename}, a[0]: {arr[0]} of type {a0_typename}'

    cloud_update_single = cloudburst_client.register(update_single, 'update_single')
    if cloud_update_single:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)

    logging.info('Function ready')

    ''' TEST REGISTERED FUNCTIONS '''
    
    for size in meta.ARR_SIZE:
        for i in [0, 999]:
            key = meta.key_gen(size, i)
            ref = CloudburstReference(key, True)
            
            # Read initial state: all zero
            res_1stread = cloud_read_single(ref).get()
            arr_1stread = np.frombuffer(res_1stread)
            if np.count_nonzero(arr_1stread):
                print(f'Unexpected result {res_1stread}, {arr_1stread} from read_single, size: {size}, idx: {i}')
                sys.exit(1)
            
            # Increment arr[0] by 1
            res_1stupdate = cloud_update_single(ref).get()
            if res_1stupdate != 1.0:
                print(f'Unexpected result {res_1stupdate} from update_single, size: {size}, idx: {i}')
                sys.exit(1)
            
            # Read again, arr[0] should be 1
            res_2ndread = cloud_read_single(ref).get()
            arr_2ndread = np.frombuffer(res_2ndread)
            if np.count_nonzero(arr_2ndread) != 1 or arr_2ndread[0] != 1.0:
                print(f'Unexpected result {res_2ndread}, {arr_2ndread} from read_single, size: {size}, idx: {i}')
                sys.exit(1)

    logging.info('Successfully tested function!')

    return [], [], [], 0
