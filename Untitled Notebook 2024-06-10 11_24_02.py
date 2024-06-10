# Databricks notebook source
# MAGIC %pip install --upgrade ipython

# COMMAND ----------

# MAGIC %pip install --upgrade pyspark

# COMMAND ----------

# Import the necessary modules
from pyspark.sql import SparkSession

# Create a new SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the JSON file into a DataFrame
df = spark.read.json('/Workspace/Repos/nitinkulkarni0@gmail.com/DBHack14/10000_random_us_people_profiles.txt')

# Register the DataFrame as a temporary table
df.createOrReplaceTempView('my_table')

# COMMAND ----------

# Run a SQL query on the table
result = spark.sql('SELECT * FROM my_table')

# Display the result
display(result)
