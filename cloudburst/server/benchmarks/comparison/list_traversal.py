import logging
import random
import sys
import time
import queue
import threading

import cloudpickle as cp
import numpy as np

from cloudburst.server.benchmarks import utils
from cloudburst.shared.reference import CloudburstReference
from cloudburst.shared.utils import DEFAULT_CLIENT_NAME

local_dag_name = 'list_traversal'

# JS rpc version
rpc_dag_name = f'rpc_{local_dag_name}'

# 1000 lists, out of 10000 numbers
UPPER_BOUND = 10000

# def gen_nodeid(id):
#     return f'{dag_name}{id}'

# def key_args():
#     return f'{dag_name}_args'

# def generate_dataset(cloudburst_client):    
#     splitter = np.sort(np.random.choice(np.arange(1, UPPER_BOUND, dtype=int), 999, replace=False)).tolist()
#     splitter = [0] + splitter
#     cloudburst_client.put_object(key_args(), splitter)
#     splitter = splitter + [0]
    
#     for offset in range(1000):
#         cur = splitter[offset]
#         next = splitter[offset + 1]
        
#         if next != 0:
#             # normal case
#             list_slice = list(range(cur, next))
#         else:
#             # last slice
#             list_slice = list(range(cur, UPPER_BOUND + 1))
        
#         # First element points to next list
#         list_slice[0] = next
        
#         cloudburst_client.put_object(gen_nodeid(cur), list_slice)
            
#     logging.info('Finished generating dataset')

def gen_nodeid(id):
    return f'{id}'

def key_args():
    return '100000'

def generate_dataset(cloudburst_client, client_name):    
    splitter = np.arange(0, UPPER_BOUND, 10).tolist()
    cloudburst_client.put_object(key_args(), splitter, client_name=client_name)
    splitter = splitter + [0]
    
    for offset in range(1000):
        cur = splitter[offset]
        next = splitter[offset + 1]
        
        if next != 0:
            # normal case
            list_slice = list(range(cur, next))
        else:
            # last slice
            list_slice = list(range(cur, UPPER_BOUND))
        
        # First element points to next list
        list_slice[0] = next
        
        cloudburst_client.put_object(gen_nodeid(cur), list_slice, client_name=client_name)
            
    logging.info('Finished generating dataset')

def create_dag(cloudburst_client):
    ''' REGISTER FUNCTIONS '''
    def list_traversal(cloudburst, nodeid, depth, client_name=DEFAULT_CLIENT_NAME):
        for i in range(depth):
            nodeid = cloudburst.get(gen_nodeid(nodeid), client_name=client_name)[0]
        return nodeid

    cloud_list_traversal = cloudburst_client.register(list_traversal, local_dag_name)
    if cloud_list_traversal:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)

    def rpc_list_traversal(cloudburst, nodeid, depth, client_name='shredder'):
        nodeid = cloudburst.execute_js_fun(local_dag_name, nodeid, depth, client_name=client_name)
        return nodeid
    
    cloud_rpc_list_traversal = cloudburst_client.register(rpc_list_traversal, rpc_dag_name)
    if cloud_rpc_list_traversal:
        logging.info('Successfully registered function.')
    else:
        print('Failed registered function.')
        sys.exit(1)
    
    ''' REGISTER DAG '''
    utils.register_dag_for_single_func(cloudburst_client, local_dag_name)
    utils.register_dag_for_single_func(cloudburst_client, rpc_dag_name)
    logging.info('Finished registering dag')
    

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 2 and args[0] != 'create':
        print(f"{args} too short. Args: kvs_name, depth")
        sys.exit(1)

    if args[0] == 'create':
        # Create dataset and DAG
        generate_dataset(cloudburst_client, DEFAULT_CLIENT_NAME)
        utils.shredder_setup_data(cloudburst_client, f'{local_dag_name}_setup')
        create_dag(cloudburst_client)
        return [], [], [], 0
    
    single_key = False
    # if len(args) >= 3:
    #     if args[-1] == 'tput':
    #         return run_tput_example(cloudburst_client, num_requests, sckt, args)
    #     else:
    #         nodeid = int(args[2])
    #         single_key = True
    if len(args) >= 3:
        nodeid = int(args[2])
        single_key = True
    
    client_name = args[0]
    depth = int(args[1])

    dag_name = rpc_dag_name if client_name == 'shredder' else local_dag_name

    nodeid_list = cloudburst_client.get_object(key_args())
    
    logging.info(f'Running {dag_name}, kvs_name {client_name}, depth {depth}, dag: {dag_name}')

    total_time = []
    epoch_req_count = 0
    epoch_latencies = []

    epoch_start = time.time()
    epoch = 0

    for _ in range(num_requests):
        if not single_key:
            nodeid = random.choice(nodeid_list)
        arg_map = {dag_name: [nodeid, depth, client_name]}
        
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
                                        (epoch), True, bname=f'{dag_name}', args=args, csv_filename='benchmark_lat.csv')

            epoch += 1
            
            epoch_req_count = 0
            epoch_latencies.clear()
            epoch_start = time.time()

    return total_time, [], [], 0

    # total_time = []
    # scheduler_time = []
    # kvs_time = []

    # retries = 0

    # log_start = time.time()

    # log_epoch = 0
    # epoch_total = []
    # epoch_req_num = 0


    # for i in range(num_requests):
    #     if not single_key:
    #         nodeid = random.choice(nodeid_list)
    #     # DAG name = Function name
    #     arg_map = {dag_name: [nodeid, depth, client_name]}
        
    #     start = time.time()
    #     res = cloudburst_client.call_dag(dag_name, arg_map)
    #     end = time.time()
    #     s_time = end - start
        
    #     start = time.time()
    #     res.get()
    #     end = time.time()
    #     k_time = end - start
        
    #     scheduler_time += [s_time]
    #     kvs_time += [k_time]
    #     epoch_total += [s_time + k_time]
    #     total_time += [s_time + k_time]
    #     epoch_req_num += 1

    #     log_end = time.time()
    #     if (log_end - log_start) > 10:
    #         if sckt:
    #             sckt.send(cp.dumps((epoch_req_num, epoch_total)))
    #         utils.print_latency_stats(epoch_total, 'EPOCH %d E2E' %
    #                                     (log_epoch), True, bname=f'{dag_name}', args=args, csv_filename='benchmark_lat.csv')

    #         epoch_req_num = 0
    #         epoch_total.clear()
    #         log_epoch += 1
    #         log_start = time.time()

    # return total_time, scheduler_time, kvs_time, retries

# def client_call_dag(cloudburst_client, stop_event, meta_dict, q, *args):
#     nodeid_list, dag_name, depth, single_nodeid = args
#     logging.info(f'dag_name: {dag_name}, depth: {depth}')
#     while True:
#         if stop_event.is_set():
#             break
#         c_id = q.get()
#         nodeid = single_nodeid if single_nodeid else random.choice(nodeid_list)
#         # DAG name = Function name
#         arg_map = {dag_name: [nodeid, depth]}
        
#         meta_dict[c_id].reset()
#         cloudburst_client.call_dag(dag_name, arg_map, direct_response=True, async_response=True, output_key=c_id)

# def run_tput_example(cloudburst_client, num_clients, sckt, args):
#     if len(args) < 2 and args[0] != 'c':
#         print(f"{args} too short. Args: kvs_name, depth")
#         sys.exit(1)

#     if args[0] == 'c':
#         # Create dataset and DAG
#         generate_dataset(cloudburst_client, DEFAULT_CLIENT_NAME)
#         utils.shredder_setup_data(cloudburst_client)
#         create_dag(cloudburst_client)
#         return [], [], [], 0
    
#     client_name = args[0]
#     depth = int(args[1])
#     if len(args) >= 4:  
#         single_nodeid = int(args[2])
#     else:
#         single_nodeid = None
#     dag_name = rpc_dag_name if client_name == 'shredder' else local_dag_name
#     nodeid_list = cloudburst_client.get_object(key_args())
#     logging.info(f'Running list traversal, kvs_name {client_name}, depth {depth}, dag: {dag_name}')
#     client_meta_dict = {}
#     profiler = utils.Profiler(bname='list_traversal', num_clients=num_clients, args=args)

#     client_q = queue.Queue(maxsize=num_clients)
#     for i in range(num_clients):
#         c_id = utils.gen_c_id(i)
#         client_q.put(c_id)
#         client_meta_dict[c_id] = utils.ClientMeta(c_id)
    
#     stop_event = threading.Event()
#     call_worker = threading.Thread(target=client_call_dag, args=(cloudburst_client, stop_event, client_meta_dict, client_q, nodeid_list, dag_name, depth, single_nodeid), daemon=True)
#     recv_worker = threading.Thread(target=utils.client_recv_dag_response, args=(cloudburst_client, stop_event, client_meta_dict, client_q, profiler), daemon=True)
    
#     call_worker.start()
#     recv_worker.start()
    
#     for i in range(5):
#         time.sleep(10)
#         epoch_tput, epoch_lat = profiler.print_tput(csv_filename='benchmark_tput.csv')
#         # send epoch result to trigger
#         sckt.send(cp.dumps((epoch_tput, epoch_lat)))

#     stop_event.set()
#     call_worker.join()
#     recv_worker.join()
    
#     return profiler.lat, [], [], 0