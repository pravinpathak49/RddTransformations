from pyspark.sql import SparkSession
from operator import add

spark = SparkSession.builder.appName('word Count').getOrCreate()
sc = spark.sparkContext

warandpeacerdd = sc.textFile('/home/hduser/Documents/RddTransformations/Data/war_and_peace.txt')
words = warandpeacerdd.flatMap(lambda s: s.split(' ')).map(lambda s: (s,1))
#wordscount = words.reduceByKey(lambda x,y: x+y).take(20)

# wordscount = words.groupByKey().map(lambda s: (s[0], add(s[1]))
wordscount = words.reduceByKey(lambda x,y: x+y)
wordscount.saveAsTextFile('/home/hduser/Documents/RddTransformations/Data/war_and_peace_wordcount.txt')
#to print in console itself. If the size is to large we don't do collect
#for item in wordscount:
#    print(item)
