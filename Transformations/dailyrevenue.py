from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Daily Revenue").set("spark.ui.port", "12901")
sc = SparkContext(conf=conf)

#reading data from LFS assigning to RDD.
orders = sc.textFile('/home/hduser/Documents/RddTransformations/Data/orders.csv')
order_items = sc.textFile('/home/hduser/Documents/RddTransformations/Data/order_items.csv')

#selecting orderid and date from orders
orderskv = orders.map(lambda s: (int(s.split(',')[0]), s.split(',')[1]))

#selecting orderid and total revenue and adding each id revenue
orderitemskv = order_items.map(lambda s: (int(s.split(',')[1]), float(s.split(',')[4]))).reduceByKey(lambda x,y: x+y)

#joining orderskv and orderitemskv
joinorderitems = orderskv.join(orderitemskv)

#selecting date and revenue and adding each date revenue also sorting  and changing tuple to csv
dailyrevenue = joinorderitems.map(lambda s: (s[1][0], s[1][1])).reduceByKey(lambda x,y: x+y).sortByKey(False).map(lambda s: s[0]+' , '+ str(s[1]))

#saving the csv to file in LFS
dailyrevenue.saveAsTextFile('/home/hduser/Documents/RddTransformations/OutputDailyRevenue')