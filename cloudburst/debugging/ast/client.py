from cloudburst.client.client import CloudburstConnection

print("Establishing connection to scheduler...")
conn = CloudburstConnection('127.0.0.1', '127.0.0.1', local=True)

cloud_sq = conn.register(lambda _, x: x * x, 'square')
print("Register success, try call function")

res = cloud_sq(2).get()
print(f"Function res: {res}")