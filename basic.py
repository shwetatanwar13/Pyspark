import sys

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Spark Count")
sc = SparkContext(conf=conf)
line=sc.textFile("carriers.csv")
l=line.collect()