from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('wordcount').getOrCreate()
sc = spark.sparkContext
sentences = ['hi how are you','i hope you are doing well','how are things going on','you are doing such a great work','i wish you tons of happiness in your life']
# Wordcount can be done in two ways
#method 1 using flatmap, map and reduceByKey
sentencesrdd = sc.parallelize(sentences)
wordcount = sentencesrdd.flatMap(lambda s: s.split(' ')).map(lambda s: (s,1)).reduceByKey(lambda x,y: x+y).collect()

# The problem with this approach is if there are n elements in collection/partition the map function will be called n no. of times.
#By using mapPartition we can increase the performance, and to pass lambda function to mapPartition we need to 
# create function as there is not flatMap in python for lambda function. 

from itertools import chain
def getwordtuples(sentences):
    wordslist = list(map(lambda s: s.split(' '),sentences))
    words = list(chain.from_iterable(wordslist)) # flatMap can be achieved 
    wordstuple = map(lambda s: (s,1), words)
    return wordstuple

wordsrdd = sentencesrdd.mapPartitions(lambda s: getwordtuples(s)) # this lambda function will be called only one per partition not per element unlike map.
wordcount = wordsrdd.reduceByKey(lambda x,y: x+y).collect()

#to submit job and test
#spark-submit --master local /home/hduser/Documents/RddTransformations/Transformations/mapvsPartition.py
