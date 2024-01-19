# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import col,row_number

data=[(1,"a@b.com"),
 (2, "c@d.com"), 
 (3, "a@b.com")]
rdd=sc.parallelize(data)
schema=("id","email")
em_df=spark.createDataFrame(rdd,schema)
em_df = em_df.dropDuplicates(["email"])
em_df.display()

# COMMAND ----------


