## NLP & Named Entity Recognition At Scale with Confluent, Databricks & Spark NLP

* This workspace contains some starter Databricks notebooks for a hypothetical use case where a **capital markets research team** wants to obtain valuable, real time insights using Named Entity Recogition models.
  * **Use Case Objective**: ingesting as much data as possible about macroeconomic events to quickly generate investment insights (and automated decision making if possible)
  * **Challenge**: hard to do this at scale and in realtime

## High Level Solution Components
* **Confluebt Kafka** for Real Time Ingestion
* **Delta Lake** Sink 
* **Spark Structured Streaming** for data cleaning and preprocessing
* **Distributed NER Model** with **Spark NLP** (for optimal throughput)
* **Realtime Visualizations** with Databricks Notebooks
