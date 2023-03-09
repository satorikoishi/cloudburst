import sys
from cloudburst.client.client import CloudburstConnection

if len(sys.argv) < 3:
    print('Usage: ./cloudburst_client.py FUNC_SERVICE_ADDR MY_IP')

local = False
elb_addr = sys.argv[1]
client_ip = sys.argv[2]

print("Establishing connection to scheduler...")
conn = CloudburstConnection(elb_addr, client_ip, local=local)

cloud_sq = conn.register(lambda _, x: x * x, 'square')
print(cloud_sq(2).get())