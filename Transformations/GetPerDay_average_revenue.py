from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Orders_Avg_revenue').getOrCreate()

#read the csv file in dataframe
OrderSchema = StructType([StructField("order_id", IntegerType()),StructField("order_date", StringType()), StructField("customer_id", IntegerType()),StructField("order_status", StringType())])
orderDF = spark.read.csv(path='/home/hduser/Documents/RddTransformations/Data/orders.csv', sep=",", schema=OrderSchema)

orderItemschema = StructType([StructField("order_item_id", IntegerType()),StructField("order_item_order_id", IntegerType()),StructField("order_item_product_id", IntegerType()),StructField("order_item_quantity", IntegerType()),StructField("order_item_subtotal", FloatType()), StructField("order_item_product_price", FloatType())])
orderItemsDF = spark.read.csv(path='/home/hduser/Documents/RddTransformations/Data/order_items.csv', sep=",", schema=orderItemschema)


# select only complete and closed records only

orderDFNew = orderDF.filter((orderDF.order_status == 'COMPLETE').__or__(orderDF.order_status=='CLOSED'))

#join date, revenue and orderid 

joinOrder_OrderItems = orderDFNew.join(orderItemsDF, orderDF.order_id == orderItemsDF.order_item_order_id).select('order_id','order_date','order_item_subtotal')

#use window functions to calculate per day average 

from pyspark.sql.window import Window

#create window specs to use in calculating per day  and per order avg revenue and total revenue respectively.

perday = Window.partitionBy('order_date')
perorder = Window.partitionBy('order_id')
avgPerDay = joinOrder_OrderItems.select('order_date','order_id',sum('order_item_subtotal').over(perorder).alias('Total_Revenue'),round(avg('order_item_subtotal').over(perday),2).alias('avgRev'))

#now get all the orderid whose revenue is greater than daily average revenue
orderGreatertheAvg = avgPerDay.filter(avgPerDay.Total_Revenue > avgPerDay.avgRev)
