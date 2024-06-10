# Databricks notebook source
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from pyspark.sql.functions import from_json, get_json_object, col

# COMMAND ----------


df = spark.sql(
    "select `name`, experience from workspace.default.profiles_11000"
)
schema = ArrayType(
            StructType(
                [
                    StructField("description", StringType()), 
                    StructField("title", StringType())
                ]
            )
        )

df = df.select(df.name, from_json(df.experience, schema).alias("experience"))
df.createOrReplaceTempView("df_raw")
print(df.count()) # 11000 rows raw data
df_filtered = spark.sql("select * from df_raw where experience is not null")
print(df_filtered.count()) # 2024 non null experience (they added prior work exp)
display(df_filtered)
df_filtered.printSchema()
df_filtered.createOrReplaceTempView("df_filtered")
spark.sql("create table workspace.default.profiles_bronze as select * from df_filtered")



# COMMAND ----------

df_bronze = spark.sql("select * from workspace.default.profiles_bronze")
display(df_bronze)
