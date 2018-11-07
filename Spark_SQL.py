
# coding: utf-8

# In[3]:


import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')


# In[4]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType,IntegerType,StructType
from pyspark import SparkConf,SparkContext


# In[5]:


data_schema = [StructField('MovieID',IntegerType()),StructField('UserID',IntegerType()),StructField('Rating',IntegerType()),StructField('DateTimeStamp',StringType())]


# In[6]:


final_struc= StructType(fields=data_schema)


# In[7]:


spark = SparkSession.builder.appName('Basics').getOrCreate()


# In[8]:


df =  spark.read.csv("u.data",schema=final_struc,sep='\t')
df


# In[9]:



df1=df.select('Rating')
df2=df1.groupBy('Rating')
df3=df2.count()
df3.show()


# In[10]:


df.printSchema()


# In[35]:


conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)


# In[36]:


lines=sc.textFile('u.data')


# In[44]:


lines.saveAsTextFile('/home/ubuntu/new')


# In[71]:


rating=lines.map(lambda x:x.split()[2])


# In[60]:


ratingcount=rating.countByValue()


# In[63]:


ratingcount.keys()


# In[59]:


sorted(ratingcount.items())


# In[54]:


import collections


# In[67]:


od=collections.OrderedDict(sorted(ratingcount.items()))


# In[70]:


for x,y in od.items():
    print(x,' ',y)


# In[11]:


df.createOrReplaceTempView("rating")


# In[15]:


spark.sql("select rating,count(*) from rating group by rating").show()


# In[22]:


gr=df.filter((df['rating'] >3) & (df['MovieID']>250)).collect()


# In[26]:


r1=gr[0]


# In[27]:


r1.asDict()


# In[29]:


df.agg({"Rating":'sum'}).show()


# In[33]:


df.orderBy(df['Rating'].desc()).show()

