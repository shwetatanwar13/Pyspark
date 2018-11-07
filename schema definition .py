
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,IntegerType,StringType


spark = SparkSession.builder.appName('movie').getOrCreate()

data_scheme=[StructField('user_id',IntegerType(),True),StructField('movie_id',IntegerType(),True),StructField('rating',IntegerType(),True),StructField('timestamp',StringType(),True)]

data_struct=StructType(fields=data_scheme)
df=spark.read.format('csv').option('header', False).option('delimiter', "\t").schema(data_struct).load('u.data')
df['user_id']


type(df.head(2)[0])

df.select(['user_id','rating']).show()




