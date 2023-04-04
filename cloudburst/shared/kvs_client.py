from anna.client import AnnaTcpClient
from redis import Redis
from cloudburst.shared.anna_ipc_client import AnnaIpcClient
from cloudburst.shared.serializer import Serializer

serializer = Serializer()

DEFAULT_KVS_NAME = 'anna'

class AbstractKvsClient():
    
    def get(self, key):
        raise NotImplementedError

    def get_list(self, keys):
        raise NotImplementedError

    def put(self, key, val):
        raise NotImplementedError

    def put_list(self, keys, vals):
        raise NotImplementedError

class AnnaKvsClient(AbstractKvsClient):
    def __init__(self, kvs_addr=None, ip=None, local=None, offset=None, anna_client=None):
        if anna_client is not None:
            self.client = anna_client
        else: self.client = AnnaTcpClient(kvs_addr, ip, local=local, offset=offset)

    def get(self, key):
        if not isinstance(key, str):
            key = str(key)
        return self.client.get(key)[key]

    def get_list(self, keys):
        keys = [str(key) for key in keys]
        return self.client.get(keys)

    def put(self, key, val):
        if not isinstance(key, str):
            key = str(key)
        return self.client.put(key, val)
    
    def put_list(self, keys, vals):
        keys = [str(key) for key in keys]
        return self.client.put(keys, vals)

    def execute_command(self, *args):
        raise RuntimeError('anna kvs client dose not impl execute_command')

class AnnaIpcKvsClient(AbstractKvsClient):
    def __init__(self, thread_id, context):
        self.client = AnnaIpcClient(thread_id, context)

    def get(self, key):
        if not isinstance(key, str):
            key = str(key)
        return self.client.get(key)[key]

    def get_list(self, keys):
        keys = [str(key) for key in keys]
        return self.client.get(keys)

    def causal_get(self, keys, future_read_set, key_version_locations, consistency, client_id):
        raise self.client.causal_get(keys, future_read_set, key_version_locations, consistency, client_id)

    def put(self, key, val):
        if not isinstance(key, str):
            key = str(key)
        return self.client.put(key, val)

    def put_list(self, keys, vals):
        keys = [str(key) for key in keys]
        return self.client.put(keys, vals)
    
    def execute_command(self, *args):
        raise RuntimeError('anna ipc kvs client dose not impl execute_command')

class RedisKvsClient(AbstractKvsClient):
    def __init__(self, host, port, db):
        self.client = Redis(host=host, port=port, db=db)

    def get(self, key):
        result = self.client.get(key)
        return serializer.load(result) if result is not None else None

    def get_list(self, keys):
        deserialized_vals = map(serializer.load, self.client.mget(keys))
        return dict(zip(keys, deserialized_vals)) # return kv pairs

    def put(self, key, val):
        data =  serializer.dump(val)
        return self.client.set(key, data) 

    def put_list(self, keys, vals):
        serialized_vals = map(serializer.dump, vals)
        kv_dict = dict(zip(keys, serialized_vals))
        return self.client.mset(kv_dict)
    
    def execute_command(self, *args):
        raise RuntimeError('redis kvs client dose not impl execute_command')

class ShredderKvsClient(AbstractKvsClient):
    def __init__(self, host, port, db):
        self.client = Redis(host=host, port=port, db=db)

    def get(self, key):
        result = self.client.get(key)
        return serializer.load(result) if result is not None else None

    # Shredder does not support `mget` operation temporarily
    def get_list(self, keys):
        values = map(self.get, keys)
        return dict(zip(keys, values))

    def put(self, key, val):
        data =  serializer.dump(val)
        return self.client.set(key, data) 

    # Shredder does not support `mset` operation temporarily
    def put_list(self, keys, vals):
        return list(map(self.put, keys, vals))

    def execute_command(self, *args):
        return self.client.execute_command(*args)
    
class KvsClient():
    def __init__(self, kvs_list):
        self.clients =  {}
        for conf in kvs_list:
            if conf['type'] == 'anna':
                self.clients[conf['name']] = AnnaKvsClient(conf['addr'], conf['ip'], conf.get('local', default=None), conf.get('offset', default=None))
            elif conf['type'] == 'anna_ipc':
                raise NotImplementedError('anna_ipc kvs client can not be created directly by KvsClient')
            elif conf['type'] == 'redis':
                self.clients[conf['name']] = RedisKvsClient(conf['host'], conf['port'], conf.get('db', default=0))
            elif conf['type'] == 'shredder':
                self.clients[conf['name']] = ShredderKvsClient(conf['host'], conf['port'], conf.get('db', default=0))
            else:
                raise ValueError('Invalid kvs type: {}'.format(conf['type']))
            
    def add_client(self, client_name, client, update=False):
        if client_name in self.clients and not update:
            raise ValueError('client name {} already exists'.format(client_name))
        self.clients[client_name] = client

    def get(self, key, client_name=DEFAULT_KVS_NAME):
        return self.clients[client_name].get(key)

    def get_list(self, keys, client_name=DEFAULT_KVS_NAME):
        return self.clients[client_name].get_list(keys)
    
    def put(self, key, val, client_name=DEFAULT_KVS_NAME):
        return self.clients[client_name].put(key, val)
    
    def put_list(self, keys, vals, client_name=DEFAULT_KVS_NAME):
        return self.clients[client_name].put_list(keys, vals)
    
    def execute_command(self, *args, client_name=DEFAULT_KVS_NAME):
        return self.clients[client_name].execute_command(*args)
    
    def get_client(self, client_name=DEFAULT_KVS_NAME):
        return self.clients[client_name]
    
    def get_client_names(self):
        return self.clients.keys()
    
    def get_client_types(self):
        return [client.__class__.__name__ for client in self.clients.values()]

    
