
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number
from pyspark.sql.functions import mean
from pyspark.sql.functions import year,month,

spark=SparkSession.builder.appName("Walmart").getOrCreate()
df=spark.read.csv("walmart_stock.csv",header=True,inferSchema=True)
df.printSchema()
df.head(5)
summary=df.describe()
summary.show()
summary.select(format_number(summary['open'].cast('float'),2)).show()

df=df.withColumn("HV Ratio",df['High']/df['Volume'])
df.select('HV Ratio').show()
df.createOrReplaceTempView("walmart")

spark.sql("select date from walmart where high=(select max(high) from walmart)").show()

df.select(mean('close')).show()

spark.sql("select max(volume),min(volume) from walmart").show()

df.filter(df['close']<60).count()
df.filter(df['high']>80).count()//df.count()
spark.sql("select ((select count(*) from walmart where high>80)/count(*)) * 100 as per from walmart").show()
df=df.withColumn('Year',year(df['date']))
df.select('Year').show()
df.groupBy('Year').max('High').show()


spark.sql("select Year,format_number(max(high),2) max_high from walmart group by Year").show()
df=df.withColumn('Month',month(df['Date']))
df.select(df['Month']).show()
df.groupBy('Month').avg('Close').orderBy('Month').show()

