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
                                        offset=10)

key = '1'
value = '2'

lattice = serializer.dump_lattice(value)
print("Writing to kvs, key = 1, value = 2")
res = kvs_client.put(key, lattice)
print(f"Write finished with {res}")

print("Reading from kvs, key = 1")
get_lattice = kvs_client.get(key)[key]
get_v = serializer.load_lattice(get_lattice)
print(f"Read get value {get_v}")