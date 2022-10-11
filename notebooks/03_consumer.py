# Databricks notebook source
!pip install --upgrade pip -q && pip install confluent-kafka -q

# COMMAND ----------

access_key = dbutils.secrets.get("confluentDemo", "nlpAccessKey")
access_secret = dbutils.secrets.get("confluentDemo", "nlpAccessSecret")
bootstrap_server = dbutils.secrets.get("confluentDemo", "nlpBoostrapServer")
topic = "financial_headline"

headlines_df = (
  spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_server)
    .option("kafka.sasl.username", access_key)
    .option("kafka.sasl.password", access_secret)
    .option("subscribe", topic)     
    .option("startingOffsets", "earliest")
    .load()
    .select("*")
  )

# COMMAND ----------

display(headlines_df.writeStream.format("console").start())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from headlines_query

# COMMAND ----------


