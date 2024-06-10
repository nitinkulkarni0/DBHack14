# Databricks notebook source
# Read data from the table into a DataFrame
df = spark.sql("SELECT * FROM your_table_name")

# Use AI functions to extract information
extracted_data = df.selectExpr("your_text_column", "ai_extract(your_text_column, array('label1', 'label2')) AS extracted_info")

# Show the extracted information
extracted_data.show()
