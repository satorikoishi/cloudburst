from anna.client import AnnaTcpClient
from redis import Redis
from cloudburst.shared.anna_ipc_client import AnnaIpcClient
from cloudburst.shared.serializer import Serializer

serializer = Serializer()

class AbstractKvsClient():
    
    def get(self, key):
        raise NotImplementedError

    def get_list(self, keys):
        raise NotImplementedError

    def put(self, key, val):
        raise NotImplementedError

class AnnaKvsClient(AbstractKvsClient):
    def __init__(self, kvs_addr, ip, local, offset):
        self.client = AnnaTcpClient(kvs_addr, ip, local=local, offset=offset)

    def get(self, key):
        return self.client.get(key)[key]

    def get_list(self, keys):
        return self.client.get(keys)

    def put(self, key, val):
        return self.client.put(key, val)

class AnnaIpcKvsClient(AbstractKvsClient):
    def __init__(self, thread_id, context):
        self.client = AnnaIpcClient(thread_id, context)

    def get(self, key):
        return self.client.get(key)[key]

    def get_list(self, keys):
        return self.client.get(keys)

    def causal_get(self, keys, future_read_set, key_version_locations, consistency, client_id):
        raise self.client.causal_get(keys, future_read_set, key_version_locations, consistency, client_id)

    def put(self, key, val):
        return self.client.put(key, val)

class RedisKvsClient(AbstractKvsClient):
    def __init__(self, host, port, db):
        self.client = Redis(host=host, port=port, db=db)

    def get(self, key):
        result = self.client.get(key)
        return serializer.load(result) if result is not None else None

    def get_list(self, keys):
        values = map(serializer.load, self.client.mget(keys))
        return dict(zip(keys, values)) # return kv pairs

    def put(self, key, val):
        data =  serializer.dump(val)
        return self.client.set(key, data)  
