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
| **Phase 06** | [Silver Layer Transformation](../phase_06/06_phase.md) |
| **Phase 07** | **Metadata-Driven OBT Transformation** |


# Phase 07 — Metadata-Driven OBT Transformation

## Overview

In this phase, the staging table is enriched by joining it with the required mapping tables.

The goal is to create the **OBT (One Big Table)**, which acts as a source of truth for the Silver Layer.

Instead of writing a separate SQL query every time a new table needs to be joined, a **metadata-driven approach using Jinja** is used.

The configuration contains:

- Source tables
- Columns to select
- Join conditions
- Optional filter conditions

Jinja uses this configuration to dynamically generate the SQL query.

---

# Step 1 — Join the Staging Table with Mapping Tables

The staging table contains the main ride information and IDs that reference other mapping tables.

For example:

```text
stg_rides
    │
    ├── vehicle_make_id
    ├── vehicle_type_id
    ├── ride_status_id
    ├── payment_method_id
    ├── pickup_city_id
    ├── dropoff_city_id
    └── cancellation_reason_id
```

These IDs can be joined with the corresponding mapping tables.

```text
                    stg_rides
                        │
        ┌───────────────┼────────────────┐
        │               │                │
        ▼               ▼                ▼
 vehicle_makes    vehicle_types    ride_statuses
        │               │                │
        └───────────────┼────────────────┘
                        │
                        ▼
                     OBT
```

The result is a single table containing the ride data together with the related lookup information.

---

# Step 2 — Metadata-Driven Pipeline

Writing a separate SQL query for every combination of tables can make the pipeline difficult to maintain.

The tutorial uses **Jinja** to create a metadata-driven pipeline.

The configuration defines what needs to be selected and joined.

```text
Metadata Configuration
        │
        ▼
     Jinja
        │
        ▼
Generated SQL
        │
        ▼
   Spark SQL
        │
        ▼
      OBT
```

This approach allows the same pipeline logic to be reused when another table needs to be added.

---

# Step 3 — Jinja SQL Template

The Jinja template acts as a SQL generator.

```python
jinja_str = """

    SELECT
        {% for config in jinja_config %}
            {{ config.select }}
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}

    FROM
        {% for config in jinja_config %}
            {% if loop.first %}
                {{ config.table }}
            {% else %}
                LEFT JOIN {{ config.table }} ON {{ config.on }}
            {% endif %}
        {% endfor %}

"""
```

The template contains loops that dynamically generate the `SELECT`, `FROM`, and `LEFT JOIN` sections of the SQL query.

---

# Step 4 — Jinja Configuration

The metadata configuration contains the tables, columns, and join conditions.

```python
jinja_config = [
    {
        "table": "uber.bronze.stg_rides stg_rides",
        "select": "stg_rides.*",
        "where": ""
    },
    {
        "table": "uber.bronze.map_vehicle_makes map_vehicle_makes",
        "select": "map_vehicle_makes.vehicle_make",
        "where": "",
        "on": "stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id"
    },
    {
        "table": "uber.bronze.map_vehicle_types map_vehicle_types",
        "select": "map_vehicle_types.vehicle_type, map_vehicle_types.description, map_vehicle_types.base_rate, map_vehicle_types.per_mile, map_vehicle_types.per_minute",
        "where": "",
        "on": "stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id"
    },
    {
        "table": "uber.bronze.map_ride_statuses map_ride_statuses",
        "select": "map_ride_statuses.ride_status, map_ride_statuses.is_completed",
        "where": "",
        "on": "stg_rides.ride_status_id = map_ride_statuses.ride_status_id"
    },
    {
        "table": "uber.bronze.map_payment_methods map_payment_methods",
        "select": "map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth",
        "where": "",
        "on": "stg_rides.payment_method_id = map_payment_methods.payment_method_id"
    },
    {
        "table": "uber.bronze.map_cities pickup_cities",
        "select": "pickup_cities.city AS pickup_city, pickup_cities.state AS pickup_state, pickup_cities.region AS pickup_region",
        "where": "",
        "on": "stg_rides.pickup_city_id = pickup_cities.city_id"
    },
    {
        "table": "uber.bronze.map_cities dropoff_cities",
        "select": "dropoff_cities.city AS dropoff_city, dropoff_cities.state AS dropoff_state, dropoff_cities.region AS dropoff_region",
        "where": "",
        "on": "stg_rides.dropoff_city_id = dropoff_cities.city_id"
    },
    {
        "table": "uber.bronze.map_cancellation_reasons map_cancellation_reasons",
        "select": "map_cancellation_reasons.cancellation_reason",
        "where": "",
        "on": "stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id"
    }
]
```

The configuration separates the **metadata** from the SQL generation logic.

---

# Step 5 — Render the Jinja Template

The Jinja template is rendered using the configuration.

```python
from jinja2 import Template

template = Template(jinja_str)

rendered_template = template.render(
    jinja_config=jinja_config
)

print(rendered_template)
```

The process is:

```text
jinja_config
     │
     ▼
Jinja Template
     │
     ▼
rendered_template
     │
     ▼
Generated SQL Query
```

---

# Step 6 — Execute the Generated SQL

The generated SQL can then be executed using Spark SQL.

```python
df = spark.sql(rendered_template)
```

The resulting DataFrame contains the staging ride data along with the selected mapping information.

---

# Step 7 — What Happens in the Pipeline?

### 1. Jinja Generates SQL

The metadata configuration provides:

- Tables
- Columns
- Join conditions
- Optional filters

Jinja uses this information to generate the SQL query.

### 2. Spark Executes the SQL

The generated SQL is passed to:

```python
spark.sql(rendered_template)
```

### 3. DataFrame is Created

Spark returns a DataFrame containing:

```text
Ride Data
    +
Vehicle Information
    +
Ride Status
    +
Payment Information
    +
Pickup City
    +
Dropoff City
    +
Cancellation Reason
```

---

# Step 8 — OBT Result

The final result is the **One Big Table (OBT)**.

```text
                    stg_rides
                        │
                        │
              ┌─────────┴─────────┐
              │                   │
              ▼                   ▼
       Mapping Tables        Join Conditions
              │                   │
              └─────────┬─────────┘
                        │
                        ▼
                 Generated SQL
                        │
                        ▼
                      OBT
                        │
                        ▼
              Silver Source of Truth
```

The OBT combines the ride records with the required lookup information into one enriched dataset.

---

# Summary

In this phase:

- The staging table is used as the main source.
- Mapping tables are joined with the staging data.
- Jinja is used to generate SQL dynamically.
- Metadata defines the tables, selected columns, and join conditions.
- The same pipeline logic can be reused when additional tables need to be included.
- Spark SQL executes the generated query.
- The resulting OBT provides an enriched source of truth for the Silver Layer.