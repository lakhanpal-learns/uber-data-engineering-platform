# Uber-Like Data Engineering Project

## Phase Navigation

| Phase | Documentation |
|---|---|
| **Phase 00** | [Project Overview & Architecture](../00_phase.md) |
| **Phase 01** | [Azure Event Hub Configuration](../phase_01/01_phase.md) |
| **Phase 02** | [Real-Time Web Application & Event Streaming](../phase_02/02_phase.md) |
| **Phase 03** | [Historical Data Ingestion with Azure Data Factory](../phase_03/03_phase.md) |
| **Phase 04** | **Event Hub to Databricks** |


# Phase 04 — Event Hub to Databricks

## Phase Objective

The objective of this phase is to connect **Azure Event Hub with Databricks** and ingest real-time ride events into the **Bronze layer**.

The overall flow is:

```text
Web Application
       │
       ▼
Azure Event Hub
       │
       │ Kafka Interface
       ▼
Databricks
       │
       ▼
Bronze Layer
```

In this phase, Databricks consumes the events from Event Hub using the **Kafka-compatible interface** provided by Azure Event Hubs.

---

# 1. Create a Databricks Free Account

Create a Databricks Free Edition account.

The tutorial uses the free edition for the project.

---

# 2. Create a Databricks Workspace

After creating the Databricks account, create a workspace.

The workspace is the environment where the project will be developed.

Conceptually:

```text
Databricks
    │
    ▼
Workspace
    │
    ├── Notebooks
    ├── Pipelines
    ├── Files
    └── Other Project Resources
```

---

# 3. Create an ETL Pipeline

Inside the Databricks workspace, create the ETL pipeline.

The pipeline will contain the code responsible for reading data from Event Hub and creating the Bronze output.

---

# 4. Create a Catalog

Create a Catalog in Databricks.

A Catalog can be thought of as a high-level container for data objects.

The structure is:

```text
Catalog
   │
   └── Schema
          │
          └── Tables
```

For this project, the Catalog is used to organize the pipeline's output.

---

# 5. Create a Schema

Create a Schema inside the Catalog.

Schema name:

```text
bronze
```

The structure becomes:

```text
Databricks
   │
   └── Catalog
          │
          └── bronze
                 │
                 └── Tables
```

The Bronze schema will contain the raw data ingested from Event Hub.

---

# 6. Create a Lakeflow Declarative Pipeline

Create a **Lakeflow Declarative Pipeline**.

The pipeline is responsible for executing the Python instructions and creating/managing the output tables.

The target location of the pipeline is configured using the Catalog and Schema.

Conceptually:

```text
Databricks
   │
   └── Workspace
          │
          └── Lakeflow Pipeline
                  │
                  └── Catalog = bronze
```

The basic idea is:

```text
Python File
     │
     │ Instructions
     ▼
Lakeflow Pipeline
     │
     │ Executes
     ▼
Output Tables
     │
     ▼
Catalog / Schema
```

### Important Concept

```text
Python File = Instructions

Pipeline = Executes those instructions

Catalog / Schema = Where pipeline output tables are stored
```

If a table cannot be created or accessed, it can be related to Unity Catalog permissions, such as missing privileges for the required schema or table operation.

---

# 7. Spark Declarative Pipeline

The tutorial uses the Spark/Lakeflow Declarative Pipeline approach.

The pipeline code defines how the data should be processed and what output should be created.

The project uses:

```python
from pyspark import pipelines as dp
```

The `dp` object provides the pipeline functionality.

For example:

```python
@dp.table
def rides_raw():
    ...
```

This tells the pipeline that the DataFrame returned by the function should become a table managed by the pipeline.

---

# 8. Create the Databricks Pipeline Code

Go to the Databricks side menu:

```text
Jobs & Pipelines
```

Create the required pipeline/project structure.

The Python code will contain the instructions for:

- Connecting to Event Hub
- Reading streaming data
- Transforming the incoming event value
- Creating the Bronze table

---

# 9. `ingestion.py`

The main ingestion code is stored in:

```text
ingestion.py
```

The code begins with the required imports.

## Import Code

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

### `pipelines as dp`

```python
from pyspark import pipelines as dp
```

This imports Spark's pipeline functionality and gives it the short name:

```text
dp
```

Then we can write:

```python
@dp.table
def customers():
    ...
```

This means:

> Define a table as part of the pipeline.

### Spark SQL/DataFrame Functions

```python
from pyspark.sql.functions import *
```

Spark provides many functions for working with DataFrames.

This import makes those functions available in the Python code.

For example:

```python
col()
```

can be used to work with DataFrame columns.

### Spark Data Types

```python
from pyspark.sql.types import *
```

This imports Spark's data types.

For example:

```python
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType())
])
```

This is similar to defining the schema of a database table.

---

# 10. Event Hub Connection Configuration

The next part of the code configures the Event Hub connection.

The Event Hub namespace and Event Hub name are retrieved from Spark configuration.

```python
EH_NAMESPACE = spark.conf.get("iot.ingestion.eh.namespace")

EH_NAME = spark.conf.get("iot.ingestion.eh.name")
```

Instead of hard-coding these values directly into the code, they are obtained from the pipeline configuration.

---

# 11. Event Hub Connection String

The Event Hub connection string is constructed using the required Event Hub information.

Conceptually:

```text
Event Hub Namespace
        │
        ├── Event Hub Name
        │
        ├── Shared Access Key Name
        │
        └── Shared Access Key
                │
                ▼
        Event Hub Connection String
```

The connection string has the structure:

```python
EH_CONN_STR = (
    f"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;"
    f"SharedAccessKeyName={EH_CONN_SHARED_ACCESS_KEY_NAME};"
    f"SharedAccessKey={EH_CONN_SHARED_ACCESS_KEY_VALUE}"
)
```

The access key value is retrieved through the configured secret mechanism.

---

# 12. Kafka Consumer Configuration

Azure Event Hubs provides a **Kafka-compatible interface**.

Therefore, Databricks can consume Event Hub data using Spark's Kafka streaming source.

The Kafka configuration is:

```python
KAFKA_OPTIONS = {
    "kafka.bootstrap.servers":
        f"{EH_NAMESPACE}.servicebus.windows.net:9093",

    "subscribe":
        EH_NAME,

    "kafka.sasl.mechanism":
        "PLAIN",

    "kafka.security.protocol":
        "SASL_SSL",

    "kafka.sasl.jaas.config":
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username=\\"$ConnectionString\\" '
        f'password=\\"{EH_CONN_STR}\\";',

    "kafka.request.timeout.ms":
        spark.conf.get(
            "iot.ingestion.kafka.requestTimeout"
        ),

    "kafka.session.timeout.ms":
        spark.conf.get(
            "iot.ingestion.kafka.sessionTimeout"
        ),

    "maxOffsetsPerTrigger":
        spark.conf.get(
            "iot.ingestion.spark.maxOffsetsPerTrigger"
        ),

    "failOnDataLoss":
        spark.conf.get(
            "iot.ingestion.spark.failOnDataLoss"
        ),

    "startingOffsets":
        spark.conf.get(
            "iot.ingestion.spark.startingOffsets"
        )
}
```

The important idea is:

```text
Azure Event Hub
       │
       │ Kafka-compatible interface
       ▼
Spark Kafka Reader
       │
       ▼
Databricks
```

---

# 13. Reading Event Hub as a Stream

The Bronze table is created using:

```python
@dp.table
def rides_raw():

    df = (
        spark.readStream
            .format("kafka")
            .options(**KAFKA_OPTIONS)
            .load()
    )

    df = df.withColumn(
        "rides",
        col("value").cast("string")
    )

    return df
```

The important part is:

```python
spark.readStream
```

This tells Spark that the source is a **streaming source**.

Then:

```python
.format("kafka")
```

tells Spark to use the Kafka source.

Because Azure Event Hubs exposes a Kafka-compatible interface, Event Hub can be consumed using this Kafka configuration.

---

# 14. Convert the Event Value to String

Kafka/Event Hub provides the event payload through the `value` column.

The code converts the value into a string:

```python
df = df.withColumn(
    "rides",
    col("value").cast("string")
)
```

Conceptually:

```text
Event Hub Event
      │
      ▼
Kafka value
      │
      ▼
cast to string
      │
      ▼
rides column
```

The resulting DataFrame is then returned:

```python
return df
```

---

# 15. Understanding `@dp.table`

This is an important concept in the Lakeflow pipeline.

When we write:

```python
@dp.table
def rides_raw():
```

we are telling the pipeline:

> The DataFrame returned by this function should become a table managed by this pipeline.

The flow is:

```text
@dp.table
     │
     ▼
"Pipeline, create/manage a table"
     │
     ▼
rides_raw()
     │
     ▼
Spark reads streaming data
     │
     ▼
DataFrame
     │
     ▼
return df
     │
     ▼
Pipeline uses returned DataFrame
     │
     ▼
rides_raw table
```

---

# 16. Transformation in the Bronze Layer

The first transformation is performed on the incoming Event Hub data.

The incoming Kafka/Event Hub value is converted to a string:

```python
df = df.withColumn(
    "rides",
    col("value").cast("string")
)
```

The resulting data is written into the Bronze table:

```text
Event Hub
    │
    ▼
Kafka Stream
    │
    ▼
rides_raw
    │
    ▼
Bronze
```

The Bronze layer is the first layer where the streaming data enters the Databricks data platform.

---

# 17. Pipeline Configuration

The pipeline also requires configuration values.

Go to the pipeline settings and select the configuration section.

The configuration contains values such as:

```text
Event Hub Namespace
Event Hub Name
Access Key Information
Kafka Settings
Starting Offset
Maximum Offsets Per Trigger
```

The Python code retrieves these values using:

```python
spark.conf.get(...)
```

This allows the code to use configuration values rather than hard-coding everything inside the Python file.

---

# 18. Explore the Data

Create a Databricks notebook to explore the incoming data.

The notebook can be used to check whether the Event Hub connection is working and whether data is being received.

Conceptually:

```text
Event Hub
    │
    ▼
Databricks
    │
    ▼
Notebook
    │
    ▼
Display Data
```

---

# 19. Checkpoints

While testing the streaming pipeline in a Databricks notebook, a checkpoint location may be used.

For example:

```python
display(
    df,
    checkpointLocation="/Volumes/uber/bronze/my_volume/volume_folder/"
)
```

A checkpoint keeps track of the progress of a streaming query.

Conceptually:

```text
Streaming Data
      │
      ▼
Spark Streaming
      │
      ├── Data
      │
      └── Checkpoint
```

The checkpoint allows the streaming process to keep track of its progress.

---

# 20. Checkpoint Management in the Lakeflow Pipeline

An important observation from the tutorial is that when using the Lakeflow Declarative Pipeline, checkpoint management is handled by the pipeline.

Therefore, the same manual checkpoint management used during notebook testing is not required in the same way inside the Lakeflow pipeline.

The notebook example may explicitly provide:

```python
checkpointLocation="..."
```

while the pipeline manages its streaming state as part of the pipeline execution.

---

# 21. Create a Volume

For the notebook testing setup, create a Volume inside the Catalog.

Example:

```text
Catalog
   │
   └── bronze
         │
         └── Volume
              │
              └── my_volume
```

Volume name:

```text
my_volume
```

Then create the required directory/folder inside the Volume.

Example:

```text
my_volume/
└── volume_folder/
```

The checkpoint path can then be:

```text
/Volumes/uber/bronze/my_volume/volume_folder/
```

---

# 22. Primary and Source Path Error

An error was encountered where the **primary and source paths must be the same**.

The issue occurred because Lakeflow was expected to use the configured pipeline source rather than attempting to reference another Python file/path outside the configured pipeline source.

The important lesson from the error is:

```text
Pipeline Source
       │
       ▼
Lakeflow Pipeline
       │
       ▼
Configured Python Files
```

The pipeline should use the configured source path and files associated with that pipeline.

---

# 23. Event Hub and Kafka Relationship

One of the key concepts learned in this phase is the relationship between Azure Event Hubs and Kafka.

Azure Event Hubs provides a Kafka-compatible interface.

Therefore, Kafka-compatible clients and Spark Kafka functionality can be used to consume Event Hub events.

Conceptually:

```text
Azure Event Hub
       │
       │ Kafka-compatible interface
       ▼
      Kafka
       │
       ▼
Databricks / Spark
```

The important idea from the tutorial is:

> The Event Hub stream can be consumed using Kafka-compatible configuration.

This allows the Databricks streaming pipeline to use:

```python
.format("kafka")
```

to consume the Event Hub data.

---

# 24. Complete Phase 04 Architecture

After completing this phase, the real-time ingestion architecture becomes:

```text
                    WEB APPLICATION
                           │
                           │
                           ▼
                    AZURE EVENT HUB
                           │
                           │
                    Kafka Interface
                           │
                           ▼
                     DATABRICKS
                           │
                           ▼
              LAKEFLOW DECLARATIVE
                    PIPELINE
                           │
                           ▼
                    rides_raw
                           │
                           ▼
                    BRONZE SCHEMA
                           │
                           ▼
                  BRONZE DATA TABLE
```

---

# 25. Phase 04 Key Concepts

The major concepts learned in this phase are:

```text
Databricks
    │
    ├── Workspace
    │
    ├── Catalog
    │
    ├── Schema
    │
    ├── Lakeflow Declarative Pipeline
    │
    ├── Streaming Table
    │
    ├── Spark Structured Streaming
    │
    ├── Kafka-Compatible Event Hub
    │
    ├── Configuration
    │
    ├── Checkpoint
    │
    └── Volume
```

The core streaming architecture is:

```text
Web Application
       │
       ▼
Azure Event Hub
       │
       ▼
Kafka-Compatible Interface
       │
       ▼
Spark readStream
       │
       ▼
Lakeflow Pipeline
       │
       ▼
rides_raw
       │
       ▼
Bronze
```

---

# 26. What We Achieved in Phase 04

By completing this phase, we established the **real-time ingestion path** of the project.

We:

- Created a Databricks environment
- Created a Databricks workspace
- Created a Catalog
- Created the Bronze Schema
- Created a Lakeflow Declarative Pipeline
- Created the `ingestion.py` pipeline code
- Configured the Event Hub connection
- Configured Kafka-compatible settings
- Used Spark Structured Streaming
- Read Event Hub using the Kafka interface
- Converted the incoming event value to a string
- Created the `rides_raw` streaming table
- Configured pipeline variables/settings
- Explored the data using a Databricks notebook
- Learned about streaming checkpoints
- Created a Volume for notebook checkpoint testing
- Understood the relationship between Event Hub and Kafka

The real-time pipeline is now:

```text
Web Application
       │
       ▼
Azure Event Hub
       │
       ▼
Databricks
       │
       ▼
Lakeflow Declarative Pipeline
       │
       ▼
rides_raw
       │
       ▼
Bronze
```

This completes the **Event Hub → Databricks Bronze ingestion stage**.