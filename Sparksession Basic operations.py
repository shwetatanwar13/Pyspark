import pyspark
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import *

sc=pyspark.SparkContext('local','basic')
lines=sc.parallelize([('a',1),('b',1),('a',2),('c',2)])
lines.foreach(lambda x:x[1])
rdd=lines.countByKey()
rdd.items()
pattern = re.compile(r'\(\'([a-z])\', ([0-9])\)') 
pattern.match("('a',1)")

def pr(x):
    print(x)


lines.foreach(pr)


stringJSONRDD = sc.parallelize((""" 
  { "id": "123",
    "name": "Katie",
    "age": 19,
    "eyeColor": "brown"
  }""",
   """{
    "id": "234",
    "name": "Michael",
    "age": 22,
    "eyeColor": "green"
  }""", 
  """{
    "id": "345",
    "name": "Simone",
    "age": 23,
    "eyeColor": "blue"
  }""")
)



spark=SparkSession.builder.appName('local').getOrCreate()
df=sc.read.json(stringJSONRDD)

df.createOrReplaceTempView("jsontable")
sc1=sc.sql("select * from jsontable")
sc1.show()
rdd=sc.parallelize([(123,'Shweta','Dark Brown','Brown'),(124,'Amol','Black','Black')])


data_schema=[StructField("id",LongType(),True),StructField("Name",StringType()),StructField("EyeColor",StringType(),True),StructField("HairColor",StringType(),True)]
data_struct=StructType(data_schema)
df=spark.createDataFrame(data=rdd,schema=data_struct)



df.show()

df.printSchema()


df.filter(df.id==123).show()
df.createOrReplaceTempView("person")
spark.sql("select id,name from person").show()
df.filter("EyeColor like 'Dark%'").show()
flights=spark.read.csv('departuredelays.csv',header=True)
flights.createOrReplaceTempView("flights")
airports=spark.read.csv('airport-codes-na.txt',header=True,sep='\t')
airports.createOrReplaceTempView("airports")

airports.filter("IATA='ABE'").show()

flights.show()

finaldf=spark.sql("select sum(delay) fdelay,a.city,f.origin from flights f, airports a where f.origin = a.iata and a.state='WA' group by a.city,f.origin order by sum(delay) desc")


type(finaldf)


import pandas


df=finaldf.toPandas()

df.plot(kind='bar')



