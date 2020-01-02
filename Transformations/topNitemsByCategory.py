from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('wordcount').getOrCreate()
sc = spark.sparkContext

#ranking-> there are two types of ranking globalranking and keybased ranking
#lets do based on crietria let's do ranking
#(2,200.0),(2,250.0),(3,150.0),(3,175.0)---Grouping--->(2,[200.0,250.0]),(3,[150.0,175.0])

products = sc.textFile('/home/hduser/Documents/RddTransformations/Data/products.csv')
productGBK = products.map(lambda s: (s.split(',')[1],s)).groupByKey()
#('10', <pyspark.resultiterable.ResultIterable at 0x7fbb6c0058d0>),
#('24', <pyspark.resultiterable.ResultIterable at 0x7fbb6c005710>),
#('17', <pyspark.resultiterable.ResultIterable at 0x7fbb6c005b00>).....

#First get the collection groupByKey here output is (key,iterables)
#take iterables and sort it and get top n items

def gettopnproducts(l, n):
    return sorted(l, key = lambda l: float(l.split(',')[4]), reverse = True)[:n]

topitemsByCategory = productGBK.Flatmap(lambda s: gettopnproducts(list(s[1]),3))
#we can also use map but map will give output as single category all three records in single list.
#['371,17,Total Gym 1900,,399.99,http://images.acmesports.sports/Total+Gym+1900',
#  '364,17,Total Gym 1400,,299.99,http://images.acmesports.sports/Total+Gym+1400',
#  '366,17,Gazelle Supreme Glider,,299.99,http://images.acmesports.sports/Gazelle+Supreme+Glider']....
topitemsByCategory.collect()
