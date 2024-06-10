# Databricks notebook source
df = spark.sql("SELECT * FROM bright_data_business_information_linkedin_listing.datasets.linked_in_people_profiles_datasets")

# COMMAND ----------

p

# COMMAND ----------

df = spark.read.json('workspace.default.10000_random_us_people_profiles')

# COMMAND ----------

df.show()

# COMMAND ----------

json_df = spark.read.json('10000_random_us_people_profiles.txt')

# Show the JSON DataFrame
json_df.show(truncate=False)

# COMMAND ----------

pdf = df.toPandas()

# COMMAND ----------

pdf.to_csv('profiles_1000.csv')

# COMMAND ----------

# Assuming 'df' is the DataFrame containing the dataset
documents = df.rdd.map(lambda row: ' '.join([str(cell) for cell in row])).collect()

# COMMAND ----------

def preprocess(text):
    text = text.replace('\n', ' ')
    return text

# COMMAND ----------

documents = []
template = """JOB DESCRIPTION:

NAME: {name}
POSITION: {position}
EXPERIENCE: {experience}
"""


# Loop through each row in the table
for index, row in pdf.iterrows():
  
  # Extract the data from the row
  name = row['name']
  position = row['position']
  experience = row['experience']

  # Insert the data into the template
  documents.append(preprocess(template.format(name=name, position=position, experience=experience)))

# COMMAND ----------

documents[2]



# COMMAND ----------

!pip install transformers torch

# COMMAND ----------

!pip install spacy


# COMMAND ----------

!pip install annoy

# COMMAND ----------

!python -m spacy download en_core_web_md

# COMMAND ----------

import spacy

# Load the spaCy model
nlp = spacy.load("en_core_web_md")

# Function to embed text using spaCy
def embed_texts(texts):
    return [nlp(text).vector for text in texts]

# Embed the preprocessed job descriptions
job_embeddings = embed_texts(documents)

# COMMAND ----------

from transformers import AutoTokenizer, AutoModel
import torch

# Load a pre-trained model and tokenizer
model_name = "sentence-transformers/all-MiniLM-L6-v2"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)

# Function to embed a list of texts
def embed_texts(texts):
    inputs = tokenizer(texts, return_tensors='pt', padding=True, truncation=True)
    with torch.no_grad():
        embeddings = model(**inputs).last_hidden_state[:, 0, :]  # Take the CLS token
    return embeddings

# Embed the preprocessed job descriptions
job_embeddings = embed_texts(documents)


# COMMAND ----------

from annoy import AnnoyIndex
import numpy as np

# Set the dimension of embeddings
embedding_dim = len(job_embeddings[0])

# Create Annoy index
index = AnnoyIndex(embedding_dim, 'angular')  # You can use 'euclidean' or 'angular'

# Add job description embeddings to the index
for i, embedding in enumerate(job_embeddings):
    index.add_item(i, embedding)

# Build the index
index.build(10)  # Number of trees

# COMMAND ----------


