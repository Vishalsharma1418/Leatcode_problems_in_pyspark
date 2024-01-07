# Databricks notebook source
# MAGIC %md
# MAGIC **Consecutive number**

# COMMAND ----------

from pyspark.sql.functions import lead
from pyspark.sql.window import Window

data=[
( 1 , 1),
(2 , 1 ),
(3 , 1 ),
(4 , 2 ),
(5 , 1 ),
(6 , 2 ),
(7 , 2 )]

schema=("id","num")
rdd=sc.parallelize(data)
con_df=spark.createDataFrame(rdd,schema)
# df.display()

window_spec=Window.orderBy(col("id"))
lead_df=con_df.withColumn("lead1",lead("num",1).over(window_spec))
leaded_df=lead_df.withColumn("lead2",lead("num",2).over(window_spec))
leaded_df.display()

result_df=leaded_df.filter((leaded_df.num==leaded_df.lead1) & (leaded_df.lead1==leaded_df.lead2))
result_df.select("num").display()
