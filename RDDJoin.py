
import findspark
findspark.init('/opt/spark-2.2.0-bin-hadoop2.7')

import pyspark
sc=pyspark.SparkContext('local','RDDJoin')

R=sc.textFile("R.txt")

R.collect()

S=sc.textFile("S.txt")
R1=R.map(lambda x:(x.split(',')[0],x.split(',')[1]))

S1=S.map(lambda x:(x.split(',')[0],x.split(',')[1]))

R1.collect()

S1.collect()

final=R1.join(S1)
final.collect()

