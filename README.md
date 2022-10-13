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

* Spark Structured Streaming
* Spark NLP
* Confluent Kafka
* Named Entity Recognition
