from anna.client import AnnaTcpClient
from redis import Redis

class KvsClient():
    def __init__(self, args, typ=AnnaTcpClient):
        self.typ = typ
        self.kvs = typ(**args)
    
    def get(self, key):
        if self.typ == AnnaTcpClient:
            return self.kvs.get(key)[key]
        elif self.typ == Redis:
            return self.kvs.get(key)
        else: raise RuntimeError(f"type {self.typ} is not supported")

    def put(self, key, val):
        if self.typ == AnnaTcpClient:
            return self.kvs.put(key, val)
        elif self.typ == Redis:
            return self.kvs.set(key, val)
        else: raise RuntimeError(f"type {self.typ} is not supported")
