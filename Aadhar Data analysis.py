
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Aadhar").getOrCreate()
df=spark.read.csv("UIDAI-ENR-DETAIL-20170308.csv",header=True,inferSchema=True)
df.groupBy("State").count().show()


df.createOrReplaceTempView("aadhar")
spark.sql("select state,count(*) no_of_identities from aadhar group by state order by count(*) desc").show()
df.groupBy('Enrolment Agency').count().orderBy('count',).show()
spark.sql("select * from (select state,count(*) no_of_identities from aadhar group by state order by count(*) desc) limit 10").show()



