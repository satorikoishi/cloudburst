import sys
from cloudburst.shared.serializer import Serializer

serializer = Serializer()

key = '1'
value = '2'

if len(sys.argv) == 3:
    key = sys.argv[1]
    value = sys.argv[2]

print(f'dump lattice with key: {key}, value: {value}')
lattice = serializer.dump_lattice(value)

print(f'load lattice')
get_v = serializer.load_lattice(lattice)
print(f"Get value {get_v}")