from anna.client import AnnaTcpClient
from redis import Redis
from cloudburst.shared.serializer import Serializer

serializer = Serializer()

class AbstractKvsClient():
    
    def get(self, key):
        raise NotImplementedError

    def put(self, key, val):
        raise NotImplementedError

class AnnaKvsClient(AbstractKvsClient):
    def __init__(self, kvs_addr, ip, local, offset):
        self.client = AnnaTcpClient(kvs_addr, ip, local=local, offset=offset)

    def get(self, key):
        return self.client.get(key)[key]

    def put(self, key, val):
        return self.client.put(key, val)

class RedisKvsClient(AbstractKvsClient):
    def __init__(self, host, port, db):
        self.client = Redis(host=host, port=port, db=db)

    def get(self, key):
        result = self.client.get(key)
        return serializer.load(result)

    def put(self, key, val):
        data =  serializer.dump(val)
        return self.client.set(key, data)
        

