import sys
from cloudburst.shared.kvs_client import ShredderKvsClient
from cloudburst.shared.serializer import Serializer

serializer = Serializer()

if len(sys.argv) < 3:
    print('Usage: python3 ./client.py Shredder_ip, ')
    exit()
    
client = ShredderKvsClient(host=sys.argv[1], port=6379, db=0)
key = sys.argv[2]

if len(sys.argv) == 4:
    repeat = int(sys.argv[3])
    print(f'Client put key {key}, value of len {repeat}')
    value = 'x' * repeat
    
    lattice = serializer.dump_lattice(value)
    res = client.put(key, lattice)
else:
    print(f'Client get key {key}')
    get_lattice = client.get(key)[key]
    res = serializer.load_lattice(get_lattice)

print(res)
