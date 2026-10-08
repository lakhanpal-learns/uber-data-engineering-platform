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
| **Phase 07** | [Metadata-Driven OBT Transformation](../phase_07/07_phase.md) |
| **Phase 08** | **Implementing CDC and Silver OBT Streaming Table** |


# Phase 08 — Implementing CDC and Silver OBT Streaming Table

## Overview

In this phase, the mapping data is updated to support change tracking.

The `updated_at` column is added to the mapping data and then passed through the Bronze and Silver layers.

The Silver OBT is also converted into a streaming table with a watermark so that it can process continuously arriving ride data.

---

# Step 1 — Add `updated_at` to Mapping Data

The `map_cities.json` file is updated by adding an `updated_at` field.

```json
{
    "updated_at": "2026-09-16T12:45:10.336+00:00"
}
```

The `updated_at` field represents the timestamp when the mapping record was last updated.

This field will later be used for change tracking and SCD Type 2 processing.

---

# Step 2 — Update the Bronze Schema

After adding the new column to the source file, the Bronze table schema also needs to support the new field.

The previous schema was:

```text
city_id
city
state
region
```

The updated schema becomes:

```text
city_id
city
state
region
updated_at
```

When overwriting a Delta table with a changed schema, `overwriteSchema` can be used.

```python
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("uber.bronze.map_cities")
```

---

# Step 3 — Update the Silver OBT

The `updated_at` column from `map_cities` is added to the Silver OBT.

```sql
pickup_cities.updated_at AS city_updated_at
```

This makes the update timestamp available in the Silver Layer.

```text
map_cities
    │
    │ updated_at
    ▼
silver_obt
    │
    │ city_updated_at
    ▼
Downstream Dimensions
```

---

# Step 4 — Create the Silver OBT Streaming Table

The Silver OBT is created as a streaming table.

```sql
CREATE OR REFRESH STREAMING TABLE silver_obt
AS

SELECT

    stg_rides.*,

    map_vehicle_makes.vehicle_make,

    map_vehicle_types.vehicle_type,
    map_vehicle_types.description,
    map_vehicle_types.base_rate,
    map_vehicle_types.per_mile,
    map_vehicle_types.per_minute,

    map_ride_statuses.ride_status,
    map_ride_statuses.is_completed,

    map_payment_methods.payment_method,
    map_payment_methods.is_card,
    map_payment_methods.requires_auth,

    pickup_cities.city AS pickup_city,
    pickup_cities.state AS pickup_state,
    pickup_cities.region AS pickup_region,
    pickup_cities.updated_at AS city_updated_at,

    dropoff_cities.city AS dropoff_city,
    dropoff_cities.state AS dropoff_state,
    dropoff_cities.region AS dropoff_region,

    map_cancellation_reasons.cancellation_reason

FROM

    STREAM (uber.bronze.stg_rides)
    WATERMARK booking_timestamp
    DELAY OF INTERVAL 3 MINUTES stg_rides

    LEFT JOIN uber.bronze.map_vehicle_makes map_vehicle_makes
        ON stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id

    LEFT JOIN uber.bronze.map_vehicle_types map_vehicle_types
        ON stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id

    LEFT JOIN uber.bronze.map_ride_statuses map_ride_statuses
        ON stg_rides.ride_status_id = map_ride_statuses.ride_status_id

    LEFT JOIN uber.bronze.map_payment_methods map_payment_methods
        ON stg_rides.payment_method_id = map_payment_methods.payment_method_id

    LEFT JOIN uber.bronze.map_cities pickup_cities
        ON stg_rides.pickup_city_id = pickup_cities.city_id

    LEFT JOIN uber.bronze.map_cities dropoff_cities
        ON stg_rides.dropoff_city_id = dropoff_cities.city_id

    LEFT JOIN uber.bronze.map_cancellation_reasons map_cancellation_reasons
        ON stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id
```

---

# Step 5 — Streaming Source and Watermark

The main streaming source is:

```sql
STREAM (uber.bronze.stg_rides)
```

This tells Databricks to process `stg_rides` as streaming input.

A watermark is also applied:

```sql
WATERMARK booking_timestamp
DELAY OF INTERVAL 3 MINUTES
```

The watermark is based on `booking_timestamp`.

It allows the streaming pipeline to handle data that arrives late within the configured delay.

---

# Step 6 — Result

The resulting Silver table contains:

```text
stg_rides
    │
    ├── Vehicle Information
    ├── Vehicle Type Information
    ├── Ride Status
    ├── Payment Information
    ├── Pickup City Information
    ├── Dropoff City Information
    ├── Cancellation Reason
    └── city_updated_at
            │
            ▼
        silver_obt
```

The `silver_obt` table becomes the source for the dimension and fact tables implemented in the next phase.