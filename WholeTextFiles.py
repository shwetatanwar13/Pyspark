
import re
import json
import pyspark
from pyspark.sql import SQLContext


sc=pyspark.SparkContext('local','wholetextfile')
jsonRDD = sc.wholeTextFiles("iotmsgs.txt").map(lambda x: x[1])

jsonRDD.take(1)

js = jsonRDD.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE))
js.collect()
sqlcontext = SQLContext(sc)
#First way to load json file
jsonDF=sqlcontext.read.json("iotmsgs.txt",multiLine=True)
jsonDF.show()
#Second way to load json file
jsonDF=sqlcontext.read.json(js)
jsonDF.show()
