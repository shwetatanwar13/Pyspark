from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("yarn-client").setAppName("Flights Data Analysis")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
flights_rdd=sc.textFile("/user/shwetatanwar13/flights_data")
f=flights_rdd.first()
flights_rdd=flights_rdd.filter(lambda x:x!=f)
airport=flights_rdd.map(lambda x:(x.split(',')[16],1))
finalrdd=airport.reduceByKey(lambda x,y:x+y)
x=finalrdd.map(lambda x:(x[1],x[0])).sortByKey().top(1)
print(type(x))
print("The airport with maximum outgoing flights is: " + str(x[0][1])+" with "+str(x[0][0])+" flights.")
