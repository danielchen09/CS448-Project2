import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

TEST_DIR = "jointests/"

dic = {}
with open(TEST_DIR + "test8.txt") as f:
    f = f.read().split("\n")
    for line in f:
        kvp = line.split(" ")
        if len(kvp) < 2:
            continue
        if kvp[0] not in dic:
            dic[kvp[0]] = []
        dic[kvp[0]].append(int(kvp[1]))

length = len(dic[list(dic.keys())[0]])
data = []
for k in dic.keys():
    data += dic[k]
    print(k + " average: " + str(sum(dic[k]) / len(dic[k])))
data = np.array(data)
data = data.reshape(length, -1)

fig,axes = plt.subplots(2,1,figsize=(10,6))
df = pd.DataFrame(data,columns=list(dic.keys()))
color = dict(boxes='DarkGreen',whiskers='DarkOrange',medians='DarkBlue',caps='Gray')

df = pd.DataFrame(dic)
ax = df.plot.line(ax=axes[0])
ax.set_xlabel('trial')
ax.set_ylabel('time in ms')
plt.legend(loc='lower left')

avgs = []
for k in dic.keys():
    avgs.append(sum(dic[k]) / len(dic[k]))
df = pd.DataFrame({'method': list(dic.keys()), 'average time in ms': avgs})
ax = df.plot.bar(x='method', y='average time in ms', ax=axes[1], rot=0)
ax.set_xlabel('')
ax.set_ylabel('time in ms')

plt.show()