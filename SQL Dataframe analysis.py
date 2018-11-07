
from pyspark.sql import SparkSession


spark=SparkSession.builder.appName("SQL").getOrCreate()



df=spark.read.csv("2015-12-01.csv",header=True)



df.printSchema()

df.show()

df.createTempView("Rtable")

spark.sql("select count(*) from Rtable where package='gridExtra'").show()


