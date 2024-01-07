# Databricks notebook source
# MAGIC %md
# MAGIC **nth highest salary**

# COMMAND ----------

# MAGIC %md
# MAGIC **nth highest salary**

# COMMAND ----------

from pyspark.sql.functions import col,rank
from pyspark.sql import SparkSession
from pyspark.sql.window import Window



#Defined data and schema
data=[(1 ,100), (2,200), (3,300)]
schema=("id" ,"salary")
rdd=sc.parallelize(data)
rdd.collect()
df=spark.createDataFrame(rdd,schema)
df.display()

def getNthHighestSalary(N):
  window_spec=Window.orderBy(col("salary").desc())
  ranked_df=df.select("salary",rank().over(window_spec).alias("rank"))
  nth_salary_df=ranked_df.filter(ranked_df.rank==N)
  display(nth_salary_df)
  nth_df=nth_salary_df.head().salary if nth_salary_df.count() > 0 else None
  return nth_df

N=3
result=getNthHighestSalary(N)
print(f"The {N}rd  hightest salary is:{result}")


# COMMAND ----------


