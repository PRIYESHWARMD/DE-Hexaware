# Databricks notebook source
import numpy as np
import pandas as pd


df = spark.table('hive_metastore.default.employees').toPandas()


# COMMAND ----------

df.shape

# COMMAND ----------

df.describe()


# COMMAND ----------

display(df)


# COMMAND ----------

duplicate_rows = df.duplicated().sum()
duplicate_columns = df.columns[df.columns.duplicated()].tolist()
print("Duplicate rows count:", duplicate_rows)
print("Duplicate columns:", duplicate_columns)

# COMMAND ----------

df = df.replace('-', np.nan)
df = df.fillna(0)


# COMMAND ----------

df['HIRE_DATE'] = pd.to_datetime(df['HIRE_DATE'], format='%d-%b-%y', errors='coerce')
display(df)


# COMMAND ----------

high_salary = df[df['SALARY'] > 5000]
display(high_salary)
dept_20 = df[df['DEPARTMENT_ID'] == 20]


# COMMAND ----------

df.sort_values('HIRE_DATE').plot(x='HIRE_DATE', y='SALARY', kind='line', title='Salary Trend Over Time')


# COMMAND ----------

df.groupby('DEPARTMENT_ID')['SALARY'].sum().plot(kind='area', title='Total Salary by Department')


# COMMAND ----------

# Convert pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(df)

# Write as Delta table
spark_df.write.format("delta").mode("overwrite").saveAsTable("hive_metastore.default.employee_delta")


# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import datetime

# Define schema explicitly
schema = StructType([
    StructField("EMPLOYEE_ID", IntegerType(), False),
    StructField("FIRST_NAME", StringType(), True),
    StructField("LAST_NAME", StringType(), True),
    StructField("EMAIL", StringType(), True),
    StructField("PHONE_NUMBER", StringType(), True),
    StructField("HIRE_DATE", DateType(), True),
    StructField("JOB_ID", StringType(), True),
    StructField("SALARY", DoubleType(), True),
    StructField("COMMISSION_PCT", DoubleType(), True),
    StructField("MANAGER_ID", IntegerType(), True),
    StructField("DEPARTMENT_ID", IntegerType(), True)
])

# Create source DataFrame with explicit schema
source = spark.createDataFrame([
    (205, "Alex", "Smith", "ASMITH", "650.123.9999", datetime.strptime("12-Jan-09", "%d-%b-%y"), "IT_PROG", 9000.0, None, 101, 60)
], schema=schema)

# Merge into Delta table
target = DeltaTable.forName(spark, "hive_metastore.default.employee_delta")

(
    target.alias("t")
    .merge(source.alias("s"), "t.EMPLOYEE_ID = s.EMPLOYEE_ID")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)


# COMMAND ----------

df_delta = spark.read.format("delta").table("hive_metastore.default.employee_delta")
display(df_delta)


# COMMAND ----------

spark_df.write.format("delta").mode("append").saveAsTable("hive_metastore.default.employee_delta")


# COMMAND ----------

target.update(
    condition="DEPARTMENT_ID = 50",
    set={"SALARY": "SALARY + 500"}
)


# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY hive_metastore.default.employee_delta"))


# COMMAND ----------

df_old = spark.read.format("delta").option("versionAsOf", 0).table("hive_metastore.default.employee_delta")
display(df_old)


# COMMAND ----------

spark.sql("OPTIMIZE hive_metastore.default.employee_delta")


# COMMAND ----------

spark.sql("OPTIMIZE hive_metastore.default.employee_delta ZORDER BY (DEPARTMENT_ID)")


# COMMAND ----------

spark.sql("VACUUM hive_metastore.default.employee_delta RETAIN 168 HOURS")
