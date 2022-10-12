# Databricks notebook source
from pyspark.sql import functions as F

display(spark.readStream.table("financial_headlines_entities").filter("entity = 'GPE'").select(F.lower("result")))

# COMMAND ----------

grouped_df = (
  spark
    .readStream
    .table("financial_headlines_entities")
    .filter("entity = 'GPE'")
    .select(F.col("result").alias("country"))
    .groupby("country")
    .count()
    .alias("count")
    .select("*")
)

display(grouped_df)

# COMMAND ----------

from pyspark.sql import functions as F

display(spark.readStream.table("financial_headlines_entities").filter("entity = 'ORG'").select(F.lower("result")))

# COMMAND ----------


