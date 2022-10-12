# Databricks notebook source
import sparknlp
from sparknlp.pretrained import PretrainedPipeline
from sparknlp.base import *

spark = sparknlp.start()

print("Spark NLP version: {}".format(sparknlp.version()))
print("Apache Spark version: {}".format(spark.version))

# COMMAND ----------

pipeline = PretrainedPipeline('onto_recognize_entities_sm')

# COMMAND ----------

sentence = """
  Burkina Faso is a country situated in Africa. 
  They have recently been in the news due to a coup d'etat;
  Captain Ibrahim Traore has been in charge since September 2022.
"""

entities = pipeline.annotate(sentence)
zipped_entities = zip(entities["token"], entities["ner"])
[entity for entity in list(zipped_entities) if entity[1] != "O"]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Named Entity Recognition at Scale

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame(data=[(sentence, 0)], schema = "text string, id int")
display(pipeline.transform(df)) 

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame(data=[(sentence, 0)], schema = "text string, id int")
transformed_df = (
  pipeline.transform(df)
    .withColumn("entities", F.explode("entities"))
    .select(
      "text",
      "entities.result",
      "entities.metadata.entity"
    )
    .where("entities.metadata.entity != 'O'")
)
display(transformed_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Named Entity Recognition in Realtime

# COMMAND ----------

df = (
  spark
    .readStream
    .table("financial_headlines")
    .withColumnRenamed("value", "text")
)

entities_df = (
  pipeline
    .transform(df)
    .withColumn("entities", F.explode("entities"))
    .select(
      "uuid",
      "text",
      "entities.result",
      "entities.metadata.entity"
    )
    .where("entities.metadata.entity != 'O'")
)

# COMMAND ----------

(
  entities_df
    .writeStream
    .format("delta")
    .trigger(processingTime='10 seconds')
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/tmp/financial_entities/checkpoints")
    .toTable("financial_entities")
)

# COMMAND ----------

display(spark.readStream.table("financial_entities"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Joining Multiple Streams

# COMMAND ----------

entities_df = spark.readStream.table("financial_entities")
headlines_df = spark.readStream.table("financial_headlines").select("uuid", "value")
joint_df = entities_df.join(headlines_df, "uuid")

# COMMAND ----------

orgs_df = joint_df.filter("entity = 'ORG'")
display(orgs_df)

# COMMAND ----------

countries_df = joint_df.filter("entity = 'GPE'")
display(countries_df)

# COMMAND ----------

(
  joint_df
    .writeStream
    .format("delta")
    .trigger(processingTime='10 seconds')
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/tmp/financial_headlines_entities/checkpoints")
    .toTable("financial_headlines_entities")
)

# COMMAND ----------


