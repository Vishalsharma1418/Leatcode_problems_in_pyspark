# Databricks notebook source
from pyspark.sql.functions import col,max
from pyspark.sql.window import Window


emp_data=[
(1 , 'Joe'  ,70000,1),            
(2 , 'Jim'  ,90000,1),            
(3 , 'Henry',80000,2),            
(4 , 'Sam'  ,60000,2),            
(5 , 'Max'  ,90000,1)]
emp_schema=('id','name','salary', 'departmentId')
dep_schema=('id','name')
dep_data=[
(1, 'IT'),    
(2, 'Sales')]
emp_rdd=sc.parallelize(emp_data)
dep_rdd=sc.parallelize(dep_data)

emp_df=spark.createDataFrame(emp_rdd,emp_schema)
# display(emp_df)

dep_df=spark.createDataFrame(dep_rdd,dep_schema)
# display(dep_df)

dep_window=Window.partitionBy("departmentId")
joindf=emp_df.join(dep_df,emp_df.departmentId==dep_df.id, 'left')
# display(joindf)

result_highest=joindf.select(dep_df["name"].alias ("Department"),
              emp_df["name"].alias("Employee"),
              emp_df["salary"],
              max(emp_df["salary"]).over(dep_window).alias('max_salary'))
display(result_highest)   

hig_df=result_highest.filter(result_highest["salary"]== col("max_salary")).select('Department',
'Employee','salary')
display(hig_df)

# COMMAND ----------


