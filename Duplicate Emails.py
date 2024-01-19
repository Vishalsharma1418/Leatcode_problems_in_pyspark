# Databricks notebook source
from pyspark.sql.functions import aggregate,count

data=[(1,"a@b.com"),
 (2, "c@d.com"), 
 (3, "a@b.com")]
# Created Rdd
rdd=sc.parallelize(data)
schema=("id","email")
# Created Dataframe
df=spark.createDataFrame(rdd,schema)
df.display()

emailcount_df=df.groupBy("email").agg(count("email").alias("email_count"))
# df_result=emailcount_df.filter(emailcount_df.email_count>1).select("email")
df_result=emailcount_df.filter(emailcount_df["email_count"]>1).select("email")

df_result.display()

# COMMAND ----------


