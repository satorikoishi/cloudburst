import logging
import random
import sys
import time

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference

# Args: k
dag_name = 'k_hop'

def gen_userid(id):
    return f'{dag_name}{id}'

# TODO: use real-world dataset
def generate_dataset(cloudburst_client):
    # 1000 users, randomly follow 0 - 100 users
    all_users = np.arange(1000)
    
    for i in range(1000):
        follow_count = random.randrange(1, 100)
        userid = gen_userid(i)
        follow_arr = np.random.choice(all_users, follow_count, replace=False)
        cloudburst_client.put_object(userid, follow_arr)
        
        # print(f'User {userid} following {follow_count} users, they are: {follow_arr}')
    
    logging.info('Finished generating dataset')

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    def k_hop(cloudburst, id, k):
        friends = cloudburst.get(gen_userid(id))
        sum = len(friends)
        
        if k == 0:
            return sum
        
        for friend_id in friends:
            sum += k_hop(cloudburst, friend_id, k - 1)
        
        return sum


    cloud_k_hop = cloudburst_client.register(k_hop, dag_name)
    if cloud_k_hop:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, dag_name)
    logging.info('Finished registering dag')
    

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 1:
        print(f"{args} too short. Args: k")
        sys.exit(1)
    
    if args[0] == 'c':
        # Create dataset and DAG
        generate_dataset(cloudburst_client)
        create_dag(cloudburst_client)
        return [], [], [], 0
    
    k = int(args[0])
    
    logging.info(f'Running k-hop with k: {k}')

    total_time = []
    scheduler_time = []
    kvs_time = []

    retries = 0

    log_start = time.time()

    log_epoch = 0
    epoch_total = []

    for i in range(num_requests):
        id = random.randrange(1000)
        # DAG name = Function name
        arg_map = {dag_name: [id, k]}
        
        start = time.time()
        res = cloudburst_client.call_dag(dag_name, arg_map)
        end = time.time()
        s_time = end - start
        
        start = time.time()
        res.get()
        end = time.time()
        k_time = end - start
        
        scheduler_time += [s_time]
        kvs_time += [k_time]
        epoch_total += [s_time + k_time]
        total_time += [s_time + k_time]

        log_end = time.time()
        if (log_end - log_start) > 10:
            if sckt:
                sckt.send(cp.dumps(epoch_total))
            utils.print_latency_stats(epoch_total, 'EPOCH %d E2E' %
                                        (log_epoch), True)

            epoch_total.clear()
            log_epoch += 1
            log_start = time.time()

    return total_time, scheduler_time, kvs_time, retries
