df1 = spark.read.csv('/home/hduser/Documents/RddTransformations/Data/orders.csv') #default column Name will be _co, _c1 and data type will be string

|-- _c0: string (nullable = true)
|-- _c1: string (nullable = true)
|-- _c2: string (nullable = true)
|-- _c3: string (nullable = true)

df1 = spark.read.csv('/home/hduser/Documents/RddTransformations/Data/orders.csv',inferSchema= True).toDF('order_id','order_date','customer_id','order_status')
#inferSchema=True is costly as it read the whole data only to get the data types of all columns. 
 |-- order_id: integer (nullable = true)
 |-- order_date: timestamp (nullable = true)
 |-- customer_id: integer (nullable = true)
 |-- order_status: string (nullable = true)


df1 = spark.read.csv('/home/hduser/Documents/RddTransformations/Data/orders.csv').toDF('order_id','order_date','customer_id','order_status') #you can pass column names with string as default data type

|-- order_id: string (nullable = true)
|-- order_date: string (nullable = true)
|-- customer_id: string (nullable = true)
|-- order_status: string (nullable = true)



#Late you can change the required datatypes of columns using withColumn() function.

from pyspark.sql.types import IntegerType
dftyped = df1. \
            withColumn('order_id', df1.order_id.cast(IntegerType())). \
            withColumn('customer_id', df1.customer_id.cast(IntegerType()))

 |-- order_id: integer (nullable = true)
 |-- order_date: timestamp (nullable = true)
 |-- customer_id: integer (nullable = true)
 |-- order_status: string (nullable = true)


#How to change the data types of columns in dataFrame. 
#df1 all columns are string but order_id and customer_id are integer. We can change this using 
#withColumn("column_name", df.column_name.cast(DoubleType())

from pyspark.sql.types import IntegerType
dftyped = df1. \
            withColumn('order_id', df1.order_id.cast(IntegerType())). \
            withColumn('customer_id', df1.customer_id.cast(IntegerType()))


#or we can predefine schema and apply while reading

schema = StructType([StructField("order_id", IntegerType()),StructField("order_date", StringType()), StructField("customer_id", IntegerType()),StructField("order_status", StringType())])

# Finally, we create the dataframe with previous variables
# Also we can specifying the separator with 'sep' (default separator for CSV is ',')


orderDF = spark.read.csv(path='/home/hduser/Documents/RddTransformations/Data/orders.csv', sep=",", schema=schema)
