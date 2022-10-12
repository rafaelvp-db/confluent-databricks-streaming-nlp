# Databricks notebook source
access_key = dbutils.secrets.get("confluentDemo", "nlpAccessKey")
access_secret = dbutils.secrets.get("confluentDemo", "nlpAccessSecret")
bootstrap_server = dbutils.secrets.get("confluentDemo", "nlpBoostrapServer")
topic = "financial_headline"

# COMMAND ----------

from pyspark.sql.functions import col, expr
from pyspark.sql.types import StringType

streamingInputDF = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", bootstrap_server)  \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{}' password='{}';".format(access_key, access_secret)) \
  .option("kafka.ssl.endpoint.identification.algorithm", "https") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("startingOffsets", "earliest") \
  .option("failOnDataLoss", "false") \
  .option("subscribe", topic) \
  .load()

query = (
  streamingInputDF
    .withColumn("key", col("key").cast(StringType()))
    .withColumn("value", col("value").cast(StringType()))
    .withColumn("uuid", expr("uuid()"))
    .writeStream
    .format("delta")
    .trigger(processingTime='10 seconds')
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/tmp/financial_headlines/checkpoints")
    .toTable("financial_headlines")
)

# COMMAND ----------

display(spark.readStream.table("financial_headlines"))

# COMMAND ----------


