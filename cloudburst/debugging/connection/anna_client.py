import sys
from anna.client import AnnaTcpClient
from cloudburst.shared.serializer import Serializer

serializer = Serializer()

if len(sys.argv) < 3:
    print('Usage: python3 ./cloudburst_client.py ROUTE_SERVICE_ADDR MY_IP')
    exit()

local = False
elb_addr = sys.argv[1]
client_ip = sys.argv[2]

kvs_client = AnnaTcpClient(elb_addr, client_ip, local=local,
                                        offset=18)
if len(sys.argv) == 3:
    key = '1'
    value = '2'

    # Write kv pair
    lattice = serializer.dump_lattice(value)
    print(f"Writing to kvs, key = {key}, value = {value}")
    res = kvs_client.put(key, lattice)
    print(f"Write finished with {res}")
else:
    # Read provided kv pair
    key = sys.argv[3]
    
print(f"Reading from kvs, key = {key}")
get_lattice = kvs_client.get(key)[key]
get_v = serializer.load_lattice(get_lattice)
print(f"Read get value = {get_v}")
