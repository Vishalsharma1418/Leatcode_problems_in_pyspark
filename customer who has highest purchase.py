# Databricks notebook source
# MAGIC %md
# MAGIC ### customer who has highest purchase

# COMMAND ----------

from pyspark.sql import SparkSession

spark1=SparkSession.builder\
  .appName("Customer")\
  .config("spark.executor.instances","2")\
  .config("spark.executor.cores","1")\
  .config("spark.executor.memory","1g")\
  .getOrCreate()


# COMMAND ----------

from pyspark.sql.functions import sum,desc
data=[(1,100,"2023-01-15"),
(2,150,"2023-02-20"),
(1,200,"2023-03-10"),
(3,50,"2023-04-05"),
(2,120,"2023-05-15"),
(1,300,"2023-06-25")]

column_name=("customer_id","purchase_amount","purchase_date")
rdd=sc.parallelize(data)
cust_df=spark.createDataFrame(rdd,column_name)
cust_df.show()

grouped_df=cust_df.groupby("customer_id").agg(sum("purchase_amount").alias("total_sum"))
result_df=grouped_df.orderBy(desc("total_sum")).first()
print("The highest tottal is :-",result_df[1])


# COMMAND ----------

print("Number of Executors:", spark1.conf.get("spark.executor.instances"))
print("Executor Cores:", spark1.conf.get("spark.executor.cores"))
print("Executor Memory:", spark1.conf.get("spark.executor.memory"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Passed list in Dataframe**

# COMMAND ----------

list=[('Vishal' ,27),("Supriya",29)]
spark.createDataFrame(list,['Name','Age']).show()

# COMMAND ----------

# spark.conf.get("spark.sql.files.maxPartitionBytes")
# df.rdd.getNumPartitions()
# spark.sparkContext.defaultParallelism
# spark.conf.get("spark.sql.files.openCostInBytes")

# COMMAND ----------

# MAGIC %md
# MAGIC **Passed dictinoary in DataFrame**

# COMMAND ----------

dict=[{'name' :'Vishal', 'age': 29} ]
spark.createDataFrame(dict).collect()
