import numpy as np

def k_hop(cloudburst, id, k):
    friends = cloudburst.get(id).tolist()
    sum = len(friends)
    
    if k == 1:
        return sum
    
    for friend_id in friends:
        sum += k_hop(cloudburst, friend_id, k - 1)
    
    return sum

def list_traversal(cloudburst, nodeid, depth):
    for i in range(depth):
        nodeid = cloudburst.get(nodeid)[0]
    return nodeid

def no_op(_):
        return 0xDEADBEEF

## Fix: should use put_set
def split(cloudburst, k):
    arr = cloudburst.get(k)
    split_arr = np.array_split(arr, 4)
    for (i, a) in enumerate(split_arr):
        cloudburst.put(f'{int(k) * 1000 + i}', a)

    return 0

## Fix: should add other logic
def accumulate(cloudburst, k):
    sum = 0
    
    for _ in range (100):
        v = cloudburst.get(k)
        sum += v

    return sum

def read_single(cloudburst, key):
    arr = cloudburst.get(key)
    return arr

def follow(cloudburst, follower_id, followee_id):
    # get info, can use get set ?
    follower_info = cloudburst.get(follower_id)
    followee_info = cloudburst.get(followee_id)
    # update info
    follower_info += 1
    followee_info += 1
    cloudburst.put(follower_id, follower_info)
    cloudburst.put(followee_id, followee_info)

# TODO: check in compose post
# def write_user_timeline(cloudburst, )