# Uber-Like Data Engineering Project

## Phase Navigation

| Phase | Documentation |
|---|---|
| **Phase 00** | [Project Overview & Architecture](../00_phase.md) |
| **Phase 01** | [Azure Event Hub Configuration](../phase_01/01_phase.md) |
| **Phase 02** | [Real-Time Web Application & Event Streaming](../phase_02/02_phase.md) |
| **Phase 03** | [Historical Data Ingestion with Azure Data Factory](../phase_03/03_phase.md) |
| **Phase 04** | [Event Hub to Databricks](../phase_04/04_phase.md) |
| **Phase 05** | **Batch Processing from Azure Data Lake to Databricks** |



# Phase 05 — Batch Processing from Azure Data Lake to Databricks

## Overview

In this phase, the historical data stored in **Azure Data Lake** is loaded into **Databricks** and stored in the **Bronze Layer**.

The data includes:

- Historical ride data
- Mapping data
- Real-time ride data

These datasets will later be combined in the **Silver Layer** to create an OBT (One Big Table).

---

## Architecture

```text
Azure Data Lake
       │
       ▼
   Databricks
       │
       ▼
  Bronze Layer
       │
       ├── Historical Ride Data
       ├── Mapping Data
       └── Real-Time Data
       │
       ▼
  Silver Layer
       │
       ▼
OBT (One Big Table)
```

---

# Step 1 — Connect Databricks to Azure Data Lake

There are different ways to connect Databricks with Azure Data Lake:

1. Access Connector
2. Public URL with Access Policy
3. Shared Access Token (SAS Token)

For this project, the **Shared Access Token (SAS Token)** method is used.

### SAS Token

The SAS token provides temporary access to files stored in Azure Data Lake.

The token is added to the storage URL as query parameters.

```text
https://<storage-account>.blob.core.windows.net/<container>/<file>.json?<SAS_TOKEN>
```

> **Note:** Never expose or commit a real SAS token to GitHub.

---

# Step 2 — Create Databricks Notebook

Create a Databricks notebook named:

```text
bronze_adls
```

The notebook is used to load the files from Azure Data Lake into the Bronze Layer.

---

# Step 3 — Define the Files

Instead of processing every file separately, the files are defined in a list.

```python
files = [
    {"file": "map_cities"},
    {"file": "map_cancellation_reasons"},
    {"file": "bulk_rides"},
    {"file": "map_payment_methods"},
    {"file": "map_ride_statuses"},
    {"file": "map_vehicle_makes"},
    {"file": "map_vehicle_types"}
]
```

This allows the same processing logic to be applied to multiple files.

---

# Step 4 — Read Files from Azure Data Lake

Each file is processed using a loop.

```python
for file in files:

    url = f"https://<storage-account>.blob.core.windows.net/raw/ingestion/{file['file']}.json?<SAS_TOKEN>"

    df = pd.read_json(url)

    df_spark = spark.createDataFrame(df)
```

### Processing Flow

```text
File List
    │
    ▼
For Loop
    │
    ▼
Generate File URL
    │
    ▼
Read JSON
    │
    ▼
Pandas DataFrame
    │
    ▼
PySpark DataFrame
```

---

# Step 5 — Convert Pandas DataFrame to PySpark

The JSON files are first read using Pandas.

```python
df = pd.read_json(url)
```

The Pandas DataFrame is then converted into a PySpark DataFrame.

```python
df_spark = spark.createDataFrame(df)
```

This allows the data to be processed and written using Spark.

---

# Step 6 — Write Data to the Bronze Layer

The PySpark DataFrame is stored as a Delta table.

```python
df_spark.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"uber.bronze.{file['file']}")
```

The mapping and historical data are therefore available in the Bronze Layer.

Example tables:

```text
uber.bronze.map_cities
uber.bronze.map_cancellation_reasons
uber.bronze.map_payment_methods
uber.bronze.map_ride_statuses
uber.bronze.map_vehicle_makes
uber.bronze.map_vehicle_types
uber.bronze.bulk_rides
```

---

# Step 7 — Delta Table

The data is stored using **Delta Lake**.

A Delta table contains:

```text
Delta Table
│
├── Data Files
│
└── _delta_log
```

The data files contain the actual records, while `_delta_log` maintains the transaction history of the Delta table.

For example:

```text
Version 0 → Initial data
Version 1 → New data added
Version 2 → Data updated
```

Delta Lake allows the table's changes and versions to be tracked.

---

# Step 8 — Bronze Layer After Batch Processing

After completing this phase, the Bronze Layer contains data from the project's different sources.

```text
                    Bronze Layer
                         │
          ┌──────────────┼──────────────┐
          │              │              │
          ▼              ▼              ▼
   Historical Data   Mapping Data   Real-Time Data
          │              │              │
          └──────────────┼──────────────┘
                         │
                         ▼
                    Silver Layer
                         │
                         ▼
                OBT (One Big Table)
```

The next phase will focus on processing these datasets in the **Silver Layer**.