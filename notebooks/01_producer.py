# Databricks notebook source
!pip install --upgrade pip -q && pip install datasets confluent-kafka -q

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Getting our Data
# MAGIC 
# MAGIC <hr></hr>
# MAGIC 
# MAGIC * We will leverage the `datasets` library from [Hugging Face](https://huggingface.co/)
# MAGIC * `financial_phrasebank` is a cureated dataset containing financial headlines about multiple topics.
# MAGIC * It is originally used for sentiment analysis, but in our case we will just use it for Named Entity Recognition.

# COMMAND ----------

from datasets import load_dataset, concatenate_datasets
from datasets.download.download_config import DownloadConfig

configs = [
  "sentences_allagree",
  "sentences_75agree",
  "sentences_66agree",
  "sentences_50agree"
]

datasets = []

for config in configs:
  dataset = load_dataset("financial_phrasebank", config)
  datasets.append(dataset["train"])
  
final_dataset = concatenate_datasets(datasets)

# COMMAND ----------

final_dataset['sentence'][:5]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Streaming our Data into Confluent Kafka
# MAGIC <hr></hr>
# MAGIC 
# MAGIC * Now that we have our data we will stream it into a Confluent Kafka cluster.
# MAGIC * This can be easily done using the Confluent Python library.
# MAGIC * Do note that you need to have previously created the following secrets to store your Confluent Kafka connection details:
# MAGIC   * `nlpAccessKey`: Confluent Kafka Access Key
# MAGIC   * `nlpAccessSecret`: Confluent Kafka Secret
# MAGIC   * `nlpBootstrapServer`: Kafka Bootstrap Server(s)

# COMMAND ----------

access_key = dbutils.secrets.get("confluentDemo", "nlpAccessKey")
access_secret = dbutils.secrets.get("confluentDemo", "nlpAccessSecret")
bootstrap_server = dbutils.secrets.get("confluentDemo", "nlpBoostrapServer")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The code below will iterate through our financial headlines dataset and will push each of the headlines into our Confluent Kafka topic.

# COMMAND ----------

from confluent_kafka import Producer
import time

# Create producer
p = Producer({
    'bootstrap.servers': bootstrap_server,
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': access_key,
    'sasl.password': access_secret,
})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

for headline in final_dataset['sentence']:
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)
    # Delay between each message
    print(f"Producing message: {headline}")
    time.sleep(1)
    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    p.produce('financial_headline', headline.encode('utf-8'), callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()

# COMMAND ----------


