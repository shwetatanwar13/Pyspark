
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField,IntegerType,StringType,StructType)


spark=SparkSession.builder.appName('Shweta').getOrCreate()
df=spark.read.json('people.json')
data_schema = [StructField('age',IntegerType(),True),StructField('name',StringType(),True)]
final_struc=StructType(fields=data_schema)
df=spark.read.json('people.json',schema=final_struc)
df.printSchema()



