# Databricks notebook source
from pyspark.sql.functions import datediff
from pyspark.sql.functions import col
ris_data=[
(1,'2015-01-01',10),          
(2,'2015-01-02',25),          
(3,'2015-01-03',20),          
(4,'2015-01-04',30)]

ris_rdd=sc.parallelize(ris_data)
schema=("id","recordDate","temperature")
ris_df=spark.createDataFrame(ris_rdd,schema)
ris_df.show()

result = ris_df.alias("w1").join(ris_df.alias("w2"),(col("w1.temperature") > col("w2.temperature")) & (datediff(col("w1.recordDate"), col("w2.recordDate")) == 1)
).select(col("w1.id"))
result.display()

# COMMAND ----------


