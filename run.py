import redis
from suanpan.api.app import getAppGraph

print(getAppGraph(54032)["connections"])

print(getAppGraph(54032)["processes"].keys())

redis_client = redis.Redis(
    host="app-54032-redis",
    port=6379,
    decode_responses=True,
    socket_keepalive=True,
    socket_connect_timeout=1,
)
m = redis_client.xrange("mq-master", "-", "+")

print(m)
for nodeid in getAppGraph(54032)["processes"].keys():
    m = redis_client.xrange(f"mq-{nodeid}", "-", "+")
    print(nodeid, m)
