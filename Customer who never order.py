# Databricks notebook source
from pyspark.sql.functions import col
customer_data=[
(1,'Joe'),
(2,'Henry'),
(3,'Sam'),
(4,'Max')]
order_data=[(1,3),(2,1 )]
customer_schema=("id","name")
order_schema=("id","customerid")

customer_rdd=sc.parallelize(customer_data)
order_rdd=sc.parallelize(order_data)
cust_df=spark.createDataFrame(customer_rdd,customer_schema)
# display(cust_df)
ord_df=spark.createDataFrame(order_rdd,order_schema)
# display(ord_df)

leftjoin_df=cust_df.join(ord_df, cust_df.id==ord_df.customerid,'left')
display(leftjoin_df)
left_result = leftjoin_df.filter(col('customerid').isNull()).select("name")


display(left_result)

# COMMAND ----------


