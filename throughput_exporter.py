import datetime
import redis
import pandas
import numpy
from functional import seq
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MultipleLocator, FormatStrFormatter
import pymongo
import sys

args = sys.argv
print('args=', args)

if(len(args) > 1):
    application = args[1]
    loadFactor = args[2]
else:
    application = 'spark'
    loadFactor = '1'

collection = pymongo.MongoClient(host="192.168.13.133")['benchmarks_viz'][application]



r = redis.Redis(host='localhost')

times = []

campaigns = r.smembers("campaigns")
for campaign in campaigns:
    windows_key = r.hget(campaign, 'windows')
    window_count = r.llen(windows_key)                # getting list size
    windows = r.lrange(windows_key, 0, window_count)  # getting list
    # print(campaign, windows_key, windows)

    for window_time in windows:
        window_key = r.hget(campaign, window_time)
        seen = r.hget(window_key, "time_updated")
        ended = int(seen)
        started = int(window_time)
        times.append(( ended, ended - started))

times.sort(key=lambda x: x[0])


interval = 10000  # 10 sec

d = seq(times)\
    .group_by(lambda x: int(x[0] / interval))\
    .map(lambda g: (datetime.datetime.fromtimestamp(g[0]*interval/1000), len(g[1])))\
    .sorted(lambda g:g[0])


#  Throughput

x = d.map(lambda x: x[0]).to_list()
y = d.map(lambda x: x[1]).to_list()

data_for_saving = [(x[i], y[i]) for i in range(len(x))]

collection.update({'_id': '{} {} {} {}'.format(application, d[0][0],'throughput',loadFactor)},
                  {'data': data_for_saving,
                   'type': 'throughput',
                   'loadFactor':loadFactor,
                   'application': '{} {}'.format(application, d[0][0]),
                   },upsert=True)

#plt.figure(figsize=(20, 10))
#plt.title('Throughput. Mean = {} Count = {} Groups = {}'.format(numpy.mean(y), sum(y), len(y)))
#plt.xlabel('Time')
#plt.ylabel('Count')
#plt.plot(x, y,'.-g')
#plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S' ))
#plt.gca().xaxis.set_minor_locator(mdates.SecondLocator())
#plt.xticks(x, x, rotation='80')
#plt.grid(axis='x')
#plt.show()



# Rolling mean Throughput

data = pandas.DataFrame([(x[i],y[i]) for i in range(len(x))], columns=['time', 'throughput'])
print(data)
data.to_csv("data/out/{}-Throughput.csv".format('{}_{}'.format(application, d[0][0])))

mean = pandas.rolling_mean(data['throughput'], 5)
print('mean',mean.tolist())
rolling_mean = pandas.DataFrame(mean.tolist(), columns=['rolling_throughput'])
x = data['time'].tolist()
y = rolling_mean['rolling_throughput'].tolist()

collection.update({'_id': '{} {} {} {}'.format(application, d[0][0],'rolling_throughput', loadFactor)},
                  {'data': [(x[i], y[i]) for i in range(len(x))],
                   'type': 'rolling_throughput',
                   'loadFactor': loadFactor,
                   'application': '{} {}'.format(application, d[0][0]),
                   }, upsert=True)

pandas.DataFrame([(x[i],y[i]) for i in range(len(x))], columns=['time', 'RollingLatency'])\
    .to_csv("data/out/{}-RollingThroughput.csv".format('{}_{}'.format(application, d[0][0])))


#plt.figure(figsize=(20, 10))
#plt.title('Rolling mean throughput. Mean = {} Count = {} Groups = {}'.format(numpy.mean(y), sum(y), len(y)))
#plt.xlabel('Time')
#plt.ylabel('Count')
#plt.plot(x,y,'.-g')
#plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S' ))
#plt.gca().xaxis.set_minor_locator(mdates.SecondLocator())
#plt.xticks(x, x, rotation='80')
#plt.grid(axis='x')
#plt.show()
