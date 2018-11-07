findspark.init('/opt/spark-2.2.0-bin-hadoop2.7')
import pyspark
sc = pyspark.SparkContext('local','Shweta')
from __future__ import print_function
import sys
from operator import add
textFile = sc.textFile("lab3-iot-gendata.txt") 

counts = textFile.flatMap(lambda x: x.split()) .map(lambda x:int(x)) .filter(lambda x: x >=89 and x<=100) .map(lambda x: (x, 1)) .reduceByKey(add)
output = counts.collect()
for (word, count) in output:
 print("%s: %i" % (word, count))


