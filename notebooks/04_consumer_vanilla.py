# Databricks notebook source
!pip install confluent-kafka

# COMMAND ----------

access_key = dbutils.secrets.get("confluentDemo", "nlpAccessKey")
access_secret = dbutils.secrets.get("confluentDemo", "nlpAccessSecret")
bootstrap_server = dbutils.secrets.get("confluentDemo", "nlpBoostrapServer")

# COMMAND ----------

from confluent_kafka import Consumer

broker = dbutils.secrets.get("confluentDemo", "nlpBoostrapServer")

config = {
  'bootstrap.servers': bootstrap_server,
  'sasl.mechanism': 'PLAIN',
  'security.protocol': 'SASL_SSL',
  'sasl.username': access_key,
  'sasl.password': access_secret,
  'group.id': 'mygroup',
  'enable.auto.commit': 'false'
}

c = Consumer(config)
c.subscribe(['financial_headline'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()

# COMMAND ----------


