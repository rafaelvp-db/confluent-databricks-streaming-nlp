## NLP & Named Entity Recognition At Scale with Confluent, Databricks & Spark NLP

* This workspace contains some starter Databricks notebooks for a hypothetical use case where a **capital markets research team** wants to obtain valuable, real time insights using Named Entity Recogition models.
  * **Use Case Objective**: ingesting as much data as possible about macroeconomic events to quickly generate investment insights (and automated decision making if possible)
  * **Challenge**: hard to do this at scale and in realtime

## Named Entity Recognition

* Natural Language Processing (NLP) / Information Retrieval (IR) technique
* Seeks to locate and classify named entities mentioned in unstructured text into pre-defined categories such as:
  * Person names
  * Organizations
  * Locations
  * Dates
  * Monetary values

### Sample Output from a NER Model

<img src="https://raw.githubusercontent.com/rafaelvp-db/confluent-databricks-streaming-nlp/main/img/ner.png" />

## High Level Solution Components
* **Confluebt Kafka** for Real Time Ingestion
* **Delta Lake** Sink 
* **Spark Structured Streaming** for data cleaning and preprocessing
* **Distributed NER Model** with **Spark NLP** (for optimal throughput)
* **Realtime Visualizations** with Databricks Notebooks

<hr></hr>

<img src="https://raw.githubusercontent.com/rafaelvp-db/confluent-databricks-streaming-nlp/main/img/arch.png" />

## Additional Reference

* [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
* [Spark NLP](https://www.johnsnowlabs.com/)
* [Confluent Kafka](https://www.confluent.io/)
* [Named Entity Recognition](https://en.wikipedia.org/wiki/Named-entity_recognition)
