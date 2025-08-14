# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("firstName", StringType(), True),
  StructField("middleName", StringType(), True),
  StructField("lastName", StringType(), True),
  StructField("gender", StringType(), True),
  StructField("birthDate", TimestampType(), True),
  StructField("ssn", StringType(), True),
  StructField("salary", IntegerType(), True)
])

df = spark.read.format("csv").option("header", True).schema(schema).load("dbfs:/mnt/hexafile/export.csv")

# Save the DataFrame as a table with overwrite mode
df.write.mode("overwrite").saveAsTable("people_10m")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date

schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("firstName", StringType(), True),
  StructField("middleName", StringType(), True),
  StructField("lastName", StringType(), True),
  StructField("gender", StringType(), True),
  StructField("birthDate", DateType(), True),
  StructField("ssn", StringType(), True),
  StructField("salary", IntegerType(), True)
])

data = [
  (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', date.fromisoformat('1992-09-17'), '953-38-9452', 55250),
  (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M', date.fromisoformat('1984-05-22'), '906-51-2137', 48500),
  (10000000, 'Joshua', 'Chas', 'Broggio', 'M', date.fromisoformat('1968-07-22'), '988-61-6247', 90000),
  (20000001, 'John', '', 'Doe', 'M', date.fromisoformat('1978-01-14'), '345-67-8901', 55500),
  (20000002, 'Mary', '', 'Smith', 'F', date.fromisoformat('1982-10-29'), '456-78-9012', 98250),
  (20000003, 'Jane', '', 'Doe', 'F', date.fromisoformat('1981-06-25'), '567-89-0123', 89900)
]

people_10m_updates = spark.createDataFrame(data, schema)
people_10m_updates.createOrReplaceTempView("people_10m_updates")

# ...

from delta.tables import DeltaTable

deltaTable = DeltaTable.forName(spark, 'people_10m')

(deltaTable.alias("people_10m")
  .merge(
    people_10m_updates.alias("people_10m_updates"),
    "people_10m.id = people_10m_updates.id")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
)

# COMMAND ----------

df = spark.read.table("people_10m")
df_filtered = df.filter(df["id"] >= 9999998)
display(df_filtered)

# COMMAND ----------

people_df = spark.read.table("people_10m")
display(people_df)

# COMMAND ----------

df.write.mode("append").saveAsTable("people_10m")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("people_10m")

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forName(spark, "people_10m")

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "gender = 'F'",
  set = { "gender": "'Female'" }
)

# Declare the predicate by using Spark SQL functions.
deltaTable.update(
  condition = col('gender') == 'M',
  set = { 'gender': lit('Male') }
)

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forName(spark, "people_10m")
display(deltaTable.history())

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forName(spark, "people_10m")
deltaHistory = deltaTable.history()

display(deltaHistory.where("version == 0"))
# Or:
display(deltaHistory.where("timestamp == '2024-05-15T22:43:15.000+00:00'"))

# COMMAND ----------

df = spark.read.option('versionAsOf', 0).table("people_10m")
# Or:
df = spark.read.option('timestampAsOf', '2025-08-11T07:18:35.000+00:00').table("people_10m")

display(df)

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forName(spark, "people_10m")
deltaTable.optimize().executeCompaction()

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forName(spark, "people_10m")
deltaTable.optimize().executeZOrderBy("gender")

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forName(spark, "people_10m")
deltaTable.vacuum()

# COMMAND ----------

from delta.tables import *

deltaTablePeople = DeltaTable.forName(spark, "people10m")
deltaTablePeopleUpdates = DeltaTable.forName(spark, "people_10m")

dfUpdates = deltaTablePeopleUpdates.toDF()

deltaTablePeople.alias('people') \
  .merge(
    dfUpdates.alias('updates'),
    'people.id = updates.id'
  ) \
  .whenMatchedUpdate(set =
    {
      "id": "updates.id",
      "firstName": "updates.firstName",
      "middleName": "updates.middleName",
      "lastName": "updates.lastName",
      "gender": "updates.gender",
      "birthDate": "updates.birthDate",
      "ssn": "updates.ssn",
      "salary": "updates.salary"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "id": "updates.id",
      "firstName": "updates.firstName",
      "middleName": "updates.middleName",
      "lastName": "updates.lastName",
      "gender": "updates.gender",
      "birthDate": "updates.birthDate",
      "ssn": "updates.ssn",
      "salary": "updates.salary"
    }
  ) \
  .execute()