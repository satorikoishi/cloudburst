# SIZE_UPPER_BOUND = 4    # For quick debugging
# SIZE_UPPER_BOUND = 7

ARR_SIZE_ANNA = [1, 8, 32, 128, 512, 2048, 8192, 32768, 131072, 524288]
ARR_SIZE_SHREDDER = [1, 8, 32, 128, 512]
ARR_SIZE = {
    'anna': ARR_SIZE_ANNA,
    'shredder': ARR_SIZE_SHREDDER
}

# for i in range(1, SIZE_UPPER_BOUND):
#     # Value size from 80B to 8MB
#     ARR_SIZE.append(pow(10, i)) 

NUM_KV_PAIRS = 1000

def key_gen(size, idx):
    # To be compatible with lattice
    return str(NUM_KV_PAIRS * size + idx)
