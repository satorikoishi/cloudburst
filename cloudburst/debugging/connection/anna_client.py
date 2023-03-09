import sys
from anna.client import AnnaTcpClient
from cloudburst.shared.serializer import Serializer

serializer = Serializer()

if len(sys.argv) < 3:
    print('Usage: ./cloudburst_client.py ROUTE_SERVICE_ADDR MY_IP')

local = False
elb_addr = sys.argv[1]
client_ip = sys.argv[2]

kvs_client = AnnaTcpClient(elb_addr, client_ip, local=local,
                                        offset=10)

lattice = serializer.dump_lattice('2')
print("Writing to kvs")
kvs_client.put('1', lattice)
print("Write finished")