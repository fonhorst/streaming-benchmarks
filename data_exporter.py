import latency_exporter
import throughput_exporter

print("Export ended")


import redis

r = redis.Redis(host='localhost')

lrange = r.lrange('events', 0, -1)
p = r.pipeline()


for l in lrange:
    p.hget(l,'event_finish_time')

data = list(p.execute())
print(len(data))