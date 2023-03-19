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
        arr = np.zeros(size)
        mem_size = sys.getsizeof(arr)
        logging.info(f'Created arr with {size} int objs, mem size {mem_size}')
        
        for i in range(meta.NUM_KV_PAIRS):
            # 1K kv pair for each size, key range [0, 1000) + size
            key = meta.key_gen(size, i)
            cloudburst_client.put_object(key, arr)

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

    logging.info('Function ready')

    ''' TEST REGISTERED FUNCTIONS '''
    
    for size in meta.ARR_SIZE:
        for i in [0, 999]:
            key = meta.key_gen(size, i)
            ref = CloudburstReference(key, True)
            res = cloud_read_single(ref).get()
            
            arr = np.frombuffer(arr)
            if np.count_nonzero(arr):
                print(f'Unexpected result {res}, {arr} from read_single, size: {size}, idx: {i}')
                sys.exit(1)

    logging.info('Successfully tested function!')

    return [], [], [], 0
