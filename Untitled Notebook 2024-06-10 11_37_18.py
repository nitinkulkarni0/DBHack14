# Databricks notebook source
# Read the jsonl file and parse the columns
df = spark.read.json("/Workspace/Repos/Yusuf.Qedan@nike.com/DBHack14/10000_random_us_people_profiles.txt", multiLine=True)

# Display the dataframe
display(df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from pyspark.sql.functions import from_json, get_json_object, col

# COMMAND ----------


df = spark.sql(
    "select `name`, experience from linkedin.datasets.linked_in_people_profiles_datasets"
)
schema = ArrayType(
            StructType(
                [
                    StructField("duration", StringType()), 
                    StructField("title", StringType())
                ]
            )
        )

df = df.select(df.name, from_json(df.experience, schema))
display(df)


# COMMAND ----------

selected_field_df = (
    df
    .select(
        get_json_object(col("experience"), "$[*]['description']").alias("description"),
        get_json_object(col("experience"), "$[*]['title']").alias("title")
    )
)
display(df)

# COMMAND ----------


