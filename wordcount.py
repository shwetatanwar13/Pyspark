import pyspark
def main():
 '''Program entry point'''
 with pyspark.SparkContext("local", "PySparkWordCount") as sc:
   lines = sc.textFile("lab3-iot-gendata.txt")
   words=lines.flatMap(lambda x:x.split()).map(lambda x:(x,1))
   wordcount=words.reduceByKey(lambda x,y:x+y)
   sum=0
   for i in wordcount.collect():
      print('Element:'+i[0]+' Count:'+ str(i[1]))
      sum=sum+i[1]
   print('Total Elements: '+str(sum))
if __name__ == "__main__":
    main()