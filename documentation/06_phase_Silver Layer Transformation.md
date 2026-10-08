# Uber-Like Data Engineering Project

## Phase Navigation

| Phase | Documentation |
|---|---|
| **Phase 00** | [Project Overview & Architecture](../00_phase.md) |
| **Phase 01** | [Azure Event Hub Configuration](../phase_01/01_phase.md) |
| **Phase 02** | [Real-Time Web Application & Event Streaming](../phase_02/02_phase.md) |
| **Phase 03** | [Historical Data Ingestion with Azure Data Factory](../phase_03/03_phase.md) |
| **Phase 04** | [Event Hub to Databricks](../phase_04/04_phase.md) |
| **Phase 05** | [Batch Processing from Azure Data Lake to Databricks](../phase_05/05_phase.md) |
| **Phase 06** | **Silver Layer Transformation** |


# Phase 06 — Silver Layer Transformation

## Overview

In this phase, the historical and real-time ride data from the Bronze Layer are brought together into a **staging table**.

The main goal is to:

- Load historical ride data.
- Continuously receive new streaming ride data.
- Combine both data sources into one staging table.
- Parse the JSON data received from Event Hub.
- Normalize the ride data.
- Build the Silver Layer transformation pipeline using **Spark Declarative Pipelines (SDP)**.

---

# Step 1 — Understanding Initial Load and Streaming

There are two types of data entering the staging layer:

1. **Initial Load** — Historical data already available in the Bronze Layer.
2. **Streaming Load** — New ride data continuously arriving from Event Hub.

```text
Historical Data
      │
      │ Initial Load
      ▼
┌─────────────────┐
│                 │
│  Staging Table  │
│                 │
└─────────────────┘
      ▲
      │ Streaming Load
      │
Real-Time Data
```

---

## Why Not Use a Normal UNION?

A normal `UNION` combines the data available at the time the query runs.

For example:

```text
Historical Data + Current Streaming Data
                │
                ▼
              UNION
```

The problem is that new streaming data will continue to arrive.

If the pipeline repeatedly performs the entire initial load together with the new streaming data, the historical data may be processed again unnecessarily.

For a very large historical dataset, this can consume significant resources.

For example:

```text
Initial Historical Data
        +
New Streaming Data
        │
        ▼
Repeated Processing
        │
        ▼
More Resource Usage
```

The solution used here is **Spark Declarative Pipelines (SDP)** with **Append Flows**.

---

# Step 2 — Append Flows in Spark Declarative Pipelines

The technique used is:

> **Append Flows / One-Time Backfill**

An empty streaming table is created first.

```python
from pyspark import pipelines as dp

dp.create_streaming_table("uber_staging")
```

The historical data is then loaded into the table once.

```python
@dp.append_flow(
    target="uber_staging",
    once=True
)
def historical():
    return spark.read.format("parquet").load(
        "/historical/"
    )
```

The `once=True` option is used for the historical backfill.

This means the historical data is loaded once instead of being repeatedly re-read as new streaming records arrive.

---

## Streaming Append Flow

The streaming data is continuously appended to the same staging table.

```python
@dp.append_flow(
    target="uber_staging"
)
def streaming():
    return (
        spark.readStream
        ...
    )
```

The overall flow becomes:

```text
Historical Data
      │
      │ once=True
      ▼
┌─────────────────────┐
│                     │
│   uber_staging      │
│                     │
└─────────────────────┘
      ▲
      │
      │ Continuous Append
      │
Streaming Data
```

Historical records are not repeatedly re-read just because new streaming records arrive.

---

# Step 3 — Create the Staging Table

The staging table used for ride data is:

```text
stg_rides
```

The table is created as an empty streaming table.

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

dp.create_streaming_table("stg_rides")
```

Two append flows are then created:

- `rides_bulk` → historical ride data
- `rides_stream` → real-time ride data

---

## Historical Ride Data

```python
@dp.append_flow(
    target="stg_rides",
    once=True
)
def rides_bulk():

    df = spark.read.table("bulk_rides")

    return df
```

The historical data is read from:

```text
bulk_rides
```

Because this is an initial historical load, `once=True` is used.

---

## Streaming Ride Data

```python
@dp.append_flow(
    target="stg_rides"
)
def rides_stream():

    df = spark.readStream.table("rides_raw")

    return df
```

The streaming data is read continuously from:

```text
rides_raw
```

---

# Step 4 — Historical and Streaming Data Flow

The complete staging flow is:

```text
                  Bronze Layer
                       │
          ┌────────────┴────────────┐
          │                         │
          ▼                         ▼
     bulk_rides                  rides_raw
    Historical Data             Streaming Data
          │                         │
          │ once=True               │ Continuous
          ▼                         ▼
       rides_bulk              rides_stream
          │                         │
          └────────────┬────────────┘
                       │
                       ▼
                  stg_rides
```

The staging table provides a common destination for both historical and streaming ride data.

---

# Step 5 — Parsing Streaming JSON Data

The historical data is already structured, but the streaming data received from Event Hub is initially stored as JSON inside a string column.

For example:

```text
rides
────────────────────────────────────────────
{"ride_id":"123","city":"Delhi","fare":250}
{"ride_id":"456","city":"Mumbai","fare":300}
```

Spark may initially see the column as:

```text
rides
────────────────
"{...}"
"{...}"
```

The JSON string needs to be converted into structured columns.

---

# Step 6 — Get the JSON Schema

The schema of the historical ride data can be used to define the structure of the streaming JSON data.

```python
df = spark.sql("SELECT * FROM uber.bronze.bulk_rides")

rides_schema = df.schema
```

The schema is obtained from the historical `bulk_rides` table.

---

# Step 7 — Parse JSON Using `from_json()`

Spark provides the `from_json()` function to convert JSON strings into structured data.

```python
df_parsed = (
    df.withColumn(
        "parsed_rides",
        from_json(col("rides"), rides_schema)
    )
    .select("parsed_rides.*")
)

display(df_parsed)
```

The process is:

```text
JSON String
     │
     ▼
 from_json()
     │
     ▼
 Struct
     │
     ▼
Individual Columns
```

---

## Understanding `from_json()`

The function receives two important inputs:

```python
from_json(
    col("rides"),
    rides_schema
)
```

### `col("rides")`

This is the column containing the JSON string.

### `rides_schema`

This defines the structure that Spark should use when parsing the JSON.

---

# Step 8 — Understanding `parsed_rides.*`

After parsing, the JSON becomes a struct.

Without:

```python
.select("parsed_rides.*")
```

the data would look conceptually like:

```text
parsed_rides
────────────────────────
{123, Delhi, 250}
{456, Mumbai, 300}
```

Using:

```python
.select("parsed_rides.*")
```

expands all fields inside the struct into individual columns.

The result becomes:

```text
ride_id | city    | fare
--------|---------|-----
123     | Delhi   | 250
456     | Mumbai  | 300
```

---

# Step 9 — Define the Ride Schema

The ride schema is explicitly defined using Spark data types.

```python
rides_schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("confirmation_number", StringType(), True),
    StructField("passenger_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("pickup_location_id", StringType(), True),
    StructField("dropoff_location_id", StringType(), True),
    StructField("vehicle_type_id", LongType(), True),
    StructField("vehicle_make_id", LongType(), True),
    StructField("payment_method_id", LongType(), True),
    StructField("ride_status_id", LongType(), True),
    StructField("pickup_city_id", LongType(), True),
    StructField("dropoff_city_id", LongType(), True),
    StructField("cancellation_reason_id", LongType(), True),
    StructField("passenger_name", StringType(), True),
    StructField("passenger_email", StringType(), True),
    StructField("passenger_phone", StringType(), True),
    StructField("driver_name", StringType(), True),
    StructField("driver_rating", DoubleType(), True),
    StructField("driver_phone", StringType(), True),
    StructField("driver_license", StringType(), True),
    StructField("vehicle_model", StringType(), True),
    StructField("vehicle_color", StringType(), True),
    StructField("license_plate", StringType(), True),
    StructField("pickup_address", StringType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("dropoff_address", StringType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("distance_miles", DoubleType(), True),
    StructField("duration_minutes", LongType(), True),
    StructField("booking_timestamp", TimestampType(), True),
    StructField("pickup_timestamp", StringType(), True),
    StructField("dropoff_timestamp", StringType(), True),
    StructField("base_fare", DoubleType(), True),
    StructField("distance_fare", DoubleType(), True),
    StructField("time_fare", DoubleType(), True),
    StructField("surge_multiplier", DoubleType(), True),
    StructField("subtotal", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("total_fare", DoubleType(), True),
    StructField("rating", DoubleType(), True)
])
```

This schema defines the expected structure and data types of the ride records.

---

# Step 10 — Implement the Transformation in `silver.py`

The transformations are now moved into the Spark Declarative Pipeline.

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

---

## Create the Staging Table

```python
dp.create_streaming_table("stg_rides")
```

---

## Historical Load

```python
@dp.append_flow(
    target="stg_rides",
    once=True
)
def rides_bulk():

    df = spark.read.table("bulk_rides")

    df = df.withColumn(
        "booking_timestamp",
        col("booking_timestamp").cast("timestamp")
    )

    return df
```

The historical data is read from `bulk_rides`.

The `booking_timestamp` column is converted into a timestamp.

---

## Streaming Load

```python
@dp.append_flow(
    target="stg_rides"
)
def rides_stream():

    df = spark.readStream.table("rides_raw")

    df_parsed = (
        df.withColumn(
            "parsed_rides",
            from_json(col("rides"), rides_schema)
        )
        .select("parsed_rides.*")
    )

    return df_parsed
```

The streaming flow:

1. Reads data from `rides_raw`.
2. Reads the JSON stored in the `rides` column.
3. Applies `rides_schema`.
4. Converts the JSON into structured data.
5. Expands the struct into individual columns.
6. Appends the records to `stg_rides`.

---

# Step 11 — Complete Staging Flow

```text
                    Bronze Layer
                         │
             ┌───────────┴───────────┐
             │                       │
             ▼                       ▼
        bulk_rides                rides_raw
        Historical                Streaming
             │                       │
             │ once=True             │
             ▼                       ▼
        rides_bulk             rides_stream
             │                       │
             │                       │
             └───────────┬───────────┘
                         ▼
                    stg_rides
                         │
                         ▼
                 Silver Transformation
                         │
                         ▼
                     silver_obt
```

---

# Step 12 — Automatic DAG in SDP

Spark Declarative Pipelines automatically understand the relationships between the different datasets and transformations.

For example:

```text
ingest.py
    │
    ▼
rides_raw
    │
    ▼
silver.py
    │
    ▼
stg_rides
    │
    ▼
silver_obt
```

This creates a dependency graph, commonly represented as a **DAG (Directed Acyclic Graph)**.

The pipeline uses these dependencies to determine the order in which the transformations need to run.

---

# Summary

In this phase:

- Historical and streaming ride data are brought into a common staging table.
- **Append Flows** are used instead of repeatedly processing the complete historical dataset.
- `once=True` is used for the historical backfill.
- Streaming data continues to append to the same staging table.
- Event Hub JSON data is converted into structured Spark columns.
- A predefined Spark schema is used with `from_json()`.
- The `parsed_rides.*` syntax expands the parsed struct into individual columns.
- The transformations are implemented using **Spark Declarative Pipelines**.
- SDP automatically builds the dependency graph between pipeline datasets.

The resulting `stg_rides` table becomes the input for the next Silver Layer transformations.