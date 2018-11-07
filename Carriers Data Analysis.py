#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init('/opt/spark-2.2.0-bin-hadoop2.7')


# In[2]:


from pyspark.sql import SparkSession


# In[3]:


spark=SparkSession.builder.appName("basic").getOrCreate()


# In[6]:


from pyspark.sql.types import StructField,StructType,StringType


# In[8]:


sch=StructType([StructField("Code",StringType()),StructField("Description",StringType(),True)])


# In[9]:


df=spark.read.csv("carriers.csv",header=True,schema=sch)


# In[12]:


df.show()


# In[13]:


df.count()


# In[34]:


df.select(df['Description']).filter("Code like '%Q'").show()


# In[23]:


df.createOrReplaceTempView("airline")


# In[25]:


spark.sql("select Description from airline where Code=16").show()


# In[35]:


df1=spark.read.load(format="csv",path="carriers.csv")


# In[40]:


df1.write.save("sh.csv")


# In[ ]:




