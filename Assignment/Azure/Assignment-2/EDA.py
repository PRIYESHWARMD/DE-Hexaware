# Databricks notebook source
import numpy as np
import pandas as pd 
df=spark.table('hive_metastore.default.owid_energy_data').toPandas() 

# COMMAND ----------

df.shape

# COMMAND ----------

df.describe()

# COMMAND ----------

df.info()

# COMMAND ----------

display(df)

# COMMAND ----------


duplicate_rows = df.duplicated().sum()


duplicate_columns = df.columns[df.columns.duplicated()].tolist()


print("Duplicate rows count:", duplicate_rows)
print("Duplicate columns:", duplicate_columns)


df = df.drop_duplicates()


df = df.loc[:, ~df.columns.duplicated()]

# COMMAND ----------

df = df.fillna(0) 

# COMMAND ----------


df['year'] = pd.to_datetime(df['year'], format='%Y', errors='coerce').dt.year

df.year.dtype
display(df)

# COMMAND ----------

display(df)