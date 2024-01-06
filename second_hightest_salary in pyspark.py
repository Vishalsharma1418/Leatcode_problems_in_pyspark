# Databricks notebook source
# MAGIC %md
# MAGIC >**Second Hightest Salary**

# COMMAND ----------

# DBTITLE 1,Generating Data
from pyspark.sql.window import Window
from pyspark.sql.functions import col,dense_rank,desc

#Defined data and schema
data=[(1 ,100), (2,200), (3,300)]
schema=("id" ,"salary")
rdd=sc.parallelize(data)
rdd.collect()
df=spark.createDataFrame(rdd,schema)
df.display()

#--------------------------Generating first highest salary-------------------------------------------------
#first method
first_df=df.select("salary").orderBy(desc("salary")).limit(1).selectExpr("IFNULL(salary,Null) AS First_hightest_salary")
display(first_df)

#second method
sec_meth_df=df.select("salary").distinct().orderBy(col("salary").desc()).limit(1).selectExpr("ifnull(salary,null) as First_hightest_salary")
display(sec_meth_df)

#--------------------------Generating second highest salary-------------------------------------------------
window=Window.orderBy(col("salary").desc())
ranked_df=df.select("salary",dense_rank().over(window).alias("dense_rank"))
display(ranked_df)

#First method
# secondhightest_df=ranked_df.filter(ranked_df.dense_rank==2)
#second method
# secondhightest_df=ranked_df.filter(ranked_df["dense_rank"]==2)
#third method
secondhightest_df=ranked_df.filter(col("dense_rank")==2)
display(secondhightest_df)

# COMMAND ----------


