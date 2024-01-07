# Databricks notebook source
# MAGIC %md
# MAGIC **Employee earn more than their employee**

# COMMAND ----------

 from pyspark.sql.functions import col
data=[
( 1 , 'Joe'   , 70000  ,3),
( 2 , 'Henry' , 80000  ,4), 
( 3 , 'Sam'   , 60000  ,'Null'), 
( 4 , 'Max'   , 90000  ,'Null')]
rdd=sc.parallelize(data)
schema=("id","name","salary","managerId")
df=spark.createDataFrame(rdd,schema)
df.display()

selfjoined_df=df.alias("e1").join(df.alias("e2"), col("e1.managerId")==col("e2.id"),"inner")
emp_df=selfjoined_df.filter(col("e1.salary")>col("e2.salary"))
result_df=emp_df.select("e1.name").alias("Emplyoee").first()[0]
print(result_df,"is this employee who earning more than their manager")
