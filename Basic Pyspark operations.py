
import sys
from operator import add
import pyspark
import collections



rdd=pyspark.SparkContext('local','movie')
lines=rdd.textFile("ml-100k/u.data")
ratings=lines.map(lambda x:(x.split()[2],1))
ratingscount=ratings.reduceByKey(add)
df=ratingscount.toLocalIterator()
ratings=lines.map(lambda x:(x.split()[2]))
rf=ratings.countByValue()
df=collections.OrderedDict(sorted(rf.items())) 

for (k,v) in sorted(rf.items()):
    print(str(k)+' '+str(v))


