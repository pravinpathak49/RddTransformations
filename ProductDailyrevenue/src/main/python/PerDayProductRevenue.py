from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import configparser as cp
import sys

props = cp.RawConfigParser()
#props.read('./src/main/resoures/application.properties')
props.read('src/main/resources/application.properties')
env = sys.argv[1]

spark = SparkSession.builder.master(props.get(env, 'executionMode')).appName('Ranking').getOrCreate()

# read the csv file in dataframe
OrderSchema = StructType([StructField("order_id", IntegerType()),StructField("order_date", StringType()), StructField("customer_id", IntegerType()),StructField("order_status", StringType())])
orderDF = spark.read.csv(path= props.get(env, 'input.base.dir')+'orders.csv', sep=",", schema=OrderSchema)

orderItemschema = StructType([StructField("order_item_id", IntegerType()), StructField("order_item_order_id", IntegerType()), StructField("order_item_product_id", IntegerType()),StructField("order_item_quantity", IntegerType()),StructField("order_item_subtotal", FloatType()), StructField("order_item_product_price", FloatType())])
orderItemsDF = spark.read.csv(path=props.get(env, 'input.base.dir') +'order_items.csv', sep=",", schema=orderItemschema)

# rank, dense_rank, row_number
# We need to create window spec using first partitionBy followed by orderBy
# 100,99,98,98,98,97
# 1,2,3,3,3,6 ->[Using rank] it will skip 4,5
# 1,2,3,3,3,4 ->[Using dense_rank] it will not skip iteration i'.e the main difference between rank and dense_rank
# 1,2,3,4,5,6 ->[Using row_number] it has to assign a unique number to each row(value)

# Problem:- Assign rank to Products based on revenue each day or month

# 1. First get OrderId and OrderDate from Orders only those orders whose order_status are completed and closed.
order = orderDF.filter((orderDF.order_status == 'COMPLETE').__or__(orderDF.order_status == 'CLOSED')).select('order_date','order_id')

# 2. Join the OrderDate with OrdeId groupBy order_date, orderId and suming the Revenue
    # 2.1 Get the order_item_order_id,order_item_product_id,Total_revenue from orderItemsDF
perOrder = Window.partitionBy('order_item_order_id')
orderItems = orderItemsDF.select('order_item_order_id','order_item_product_id', sum(orderItemsDF.order_item_subtotal).over(perOrder).alias('Total_revenue'))
    # 2.2 Join orderitem and order based on id and groupBy daily basis
orderRevenueDaily = order.join(orderItems, order.order_id==orderItems.order_item_order_id)
    # 2.3 GroupBy Order_date and Product_id and get per day per_product revenue
ProductRevenueDaily = orderRevenueDaily.groupBy('order_date', 'order_item_product_id').sum('Total_revenue')
# ProductRevenueDaily.show()

# 3. Order the records based on Revenue
    # 3.1 Create windowSpec object to Rank
perDay = Window.partitionBy('order_date').orderBy('sum(Total_revenue)')
    # 3.2 Rank the ProductRevenueDaily
RankedProductsId = ProductRevenueDaily.select('order_date', 'order_item_product_id', 'sum(Total_revenue)', rank().over(perDay).alias('Rank'))
# 4 Select Top n items
#RankedProductsId.write.csv(path='/home/hduser/Documents/RddTransformations/Output/')
RankedProductsId.coalesce(1).write.csv(path=props.get(env, 'output.base.dir') +'ProductDailyRevenue')