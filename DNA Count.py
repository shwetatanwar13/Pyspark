import pyspark

sc=pyspark.SparkContext('local','DNA')
rdd=sc.textFile('dna.txt')

def countdna(x):
    dict1=dict()
    for i in x:
        if i in dict1.keys():
            dict1[i]+=1
        else:
            dict1[i]=1
    return dict1


rdd1=rdd.map(countdna)
rdd2=rdd1.flatMap(lambda x:[(k,v) for k,v in x.items()])

import operator
final=rdd2.reduceByKey(operator.add)
final.collect()

for i in sorted(final):
    print(i)




