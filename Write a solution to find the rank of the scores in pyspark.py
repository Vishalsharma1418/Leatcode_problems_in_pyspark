# Databricks notebook source
# MAGIC %md
# MAGIC **Give a Rank**
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col,dense_rank
from pyspark.sql.window import Window

data=[( 1,3.50),
(2 ,3.65),
(3 ,4.00),
(4 ,3.85),
(5 ,4.00),
(6 ,3.65)]

rdd=sc.parallelize(data)
rank_schema=('id','scores')
rank_df=spark.createDataFrame(rdd,rank_schema)
display(rank_df)

window_spec=Window.orderBy(col("scores").desc())
ranked_df=rank_df.withColumn("ranks", dense_rank().over(window_spec))
ranked_df.display()

