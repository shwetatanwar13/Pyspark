
import pyspark

sc=pyspark.SparkContext('local','bigrams')
rdd=sc.textFile("data.txt")
rdd.collect()
rdd1=rdd.flatMap(lambda x:[((x.split()[i],x.split()[i+1]),1) for i in (0,len(x.split())-2)])
rdd1=rdd.map(lambda x:x.split()).flatMap(lambda x:[((x[i-1],x[i]),1) for i in range(1,len(x))])
rdd1.reduceByKey(lambda x,y:x+y).collect()

