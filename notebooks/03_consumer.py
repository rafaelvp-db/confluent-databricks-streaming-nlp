# Databricks notebook source
!rm -rf /dbfs/tmp/financial/checkpoints
spark.sql("drop table if exists financial_headline123")

# COMMAND ----------

access_key = dbutils.secrets.get("confluentDemo", "nlpAccessKey")
access_secret = dbutils.secrets.get("confluentDemo", "nlpAccessSecret")
bootstrap_server = dbutils.secrets.get("confluentDemo", "nlpBoostrapServer")
topic = "financial_headline"

# COMMAND ----------

headlines_df = headlines_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# COMMAND ----------

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
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only)
    .queryName("raw")        # raw = name of the in-memory table
    .outputMode("append")    # append = add new events#
    .trigger(processingTime='5 seconds')
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select CAST(key as STRING) as key, CAST(value as STRING) as value from raw

# COMMAND ----------


