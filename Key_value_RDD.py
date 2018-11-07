

import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')
import pyspark
from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

line=sc.textFile("fakefriends.csv")
line.collect()

rdd=line.map(lambda x:(x.split(',')[2],int(x.split(',')[3])))

rdd1=rdd.mapValues(lambda x:(x,1))

rdd2=rdd1.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))

rdd3=rdd2.mapValues(lambda x:(x[0]/x[1]))
rdd3.collect()

