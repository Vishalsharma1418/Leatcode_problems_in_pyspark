# Databricks notebook source
from pyspark.sql.functions import min
gam_data=[
(1 ,2,'2016-03-01', 5),
(1 ,2,'2016-05-02', 6),
(2 ,3,'2017-06-25', 1),
(3 ,1,'2016-03-02', 0),
(3 ,4,'2018-07-03', 5)]

gam_rdd=sc.parallelize(gam_data)
gam_schema=("player_id","device_id","event_date","games_played")
gam_df=spark.createDataFrame(gam_rdd,gam_schema)
display(gam_df)
fr_df=gam_df.groupBy("player_id").agg(min("event_date").alias("first_logindate"))
fr_df.display()

# COMMAND ----------


