SIZE_UPPER_BOUND = 7

ARR_SIZE = []

for i in range(1, SIZE_UPPER_BOUND):
    # Value size from 80B to 8MB
    ARR_SIZE.append(pow(10, i)) 

NUM_KV_PAIRS = 1000

def key_gen(size, idx):
    # To compatible with lattice
    return str(NUM_KV_PAIRS * size + idx)