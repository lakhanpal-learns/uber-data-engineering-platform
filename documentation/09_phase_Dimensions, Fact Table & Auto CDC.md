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
| **Phase 08** | [Implementing CDC and Silver OBT Streaming Table](../phase_08/08_phase.md) |
| **Phase 09** | **Dimensions, Fact Table & Auto CDC** |



# Phase 09 — Dimensions, Fact Table & Auto CDC

## Overview

In this phase, the Silver OBT is used to create the dimension and fact tables.

For each dimension:

1. A streaming view is created.
2. Required columns are selected.
3. A streaming table is created.
4. An Auto CDC flow is created.
5. The required key and sequence column are defined.
6. The SCD type is specified.

The same pattern is used for the different dimensions and the fact table.

---

# Step 1 — Create the Passenger View

The passenger data is read continuously from `silver_obt`.

```python
from pyspark import pipelines as dp

@dp.view
def dim_passenger_view():

    df = spark.readStream.table("silver_obt")

    df = df.select(
        "passenger_id",
        "passenger_name",
        "passenger_email",
        "passenger_phone"
    )

    return df
```

The view acts as the source for the Auto CDC flow.

```text
silver_obt
    │
    ▼
dim_passenger_view
```

---

# Step 2 — Create the Passenger Streaming Table

The target streaming table is created using:

```python
dp.create_streaming_table("dim_passenger")
```

The flow becomes:

```text
silver_obt
    │
    ▼
dim_passenger_view
    │
    ▼
dim_passenger
```

---

# Step 3 — Create Auto CDC Flow

Auto CDC connects the source view with the target table.

```python
dp.create_auto_cdc_flow(
    target="dim_passenger",
    source="dim_passenger_view",
    keys=["passenger_id"],
    sequence_by="passenger_id",
    stored_as_scd_type=1,
)
```

The configuration defines:

| Parameter | Purpose |
|---|---|
| `target` | Target streaming table |
| `source` | Source view |
| `keys` | Identifies the record |
| `sequence_by` | Determines the order of changes |
| `stored_as_scd_type` | Defines the SCD type |

---

# Step 4 — Create Driver Dimension

The same pattern is used for the driver dimension.

```python
@dp.view
def dim_driver_view():

    df = spark.readStream.table("silver_obt")

    df = df.select(
        "driver_id",
        "driver_name",
        "driver_rating",
        "driver_phone",
        "driver_license"
    )

    return df
```

Create the target table:

```python
dp.create_streaming_table("dim_driver")
```

Create the Auto CDC flow:

```python
dp.create_auto_cdc_flow(
    target="dim_driver",
    source="dim_driver_view",
    keys=["driver_id"],
    sequence_by="driver_id",
    stored_as_scd_type=1,
)
```

---

# Step 5 — Create Vehicle Dimension

```python
@dp.view
def dim_vehicle_view():

    df = spark.readStream.table("silver_obt")

    df = df.select(
        "vehicle_id",
        "vehicle_make_id",
        "vehicle_type_id",
        "vehicle_model",
        "vehicle_color",
        "license_plate",
        "vehicle_make",
        "vehicle_type"
    )

    return df
```

Create the target table:

```python
dp.create_streaming_table("dim_vehicle")
```

Create the Auto CDC flow:

```python
dp.create_auto_cdc_flow(
    target="dim_vehicle",
    source="dim_vehicle_view",
    keys=["vehicle_id"],
    sequence_by="vehicle_id",
    stored_as_scd_type=1,
)
```

---

# Step 6 — Create Payment Dimension

```python
@dp.view
def dim_payment_view():

    df = spark.readStream.table("silver_obt")

    df = df.select(
        "payment_method_id",
        "payment_method",
        "is_card",
        "requires_auth"
    )

    return df
```

Create the target table:

```python
dp.create_streaming_table("dim_payment")
```

Create the Auto CDC flow:

```python
dp.create_auto_cdc_flow(
    target="dim_payment",
    source="dim_payment_view",
    keys=["payment_method_id"],
    sequence_by="payment_method_id",
    stored_as_scd_type=1,
)
```

---

# Step 7 — Create Booking Dimension

```python
@dp.view
def dim_booking_view():

    df = spark.readStream.table("silver_obt")

    df = df.select(
        "ride_id",
        "confirmation_number",
        "dropoff_location_id",
        "ride_status_id",
        "dropoff_city_id",
        "cancellation_reason_id",
        "dropoff_address",
        "dropoff_latitude",
        "dropoff_longitude",
        "booking_timestamp",
        "dropoff_timestamp",
        "pickup_address",
        "pickup_latitude",
        "pickup_longitude",
        "pickup_location_id"
    )

    return df
```

Create the target table:

```python
dp.create_streaming_table("dim_booking")
```

Create the Auto CDC flow:

```python
dp.create_auto_cdc_flow(
    target="dim_booking",
    source="dim_booking_view",
    keys=["ride_id"],
    sequence_by="ride_id",
    stored_as_scd_type=1,
)
```

---

# Step 8 — Create Location Dimension

The location dimension uses `city_updated_at` as the sequence column.

```python
@dp.view
def dim_location_view():

    df = spark.readStream.table("silver_obt")

    df = df.select(
        "pickup_city_id",
        "pickup_city",
        "pickup_region",
        "pickup_state",
        "city_updated_at"
    )

    return df
```

Create the target table:

```python
dp.create_streaming_table("dim_location")
```

Create the Auto CDC flow:

```python
dp.create_auto_cdc_flow(
    target="dim_location",
    source="dim_location_view",
    keys=["pickup_city_id"],
    sequence_by="city_updated_at",
    stored_as_scd_type=2,
)
```

Here:

```text
pickup_city_id
       │
       ▼
Record Key

city_updated_at
       │
       ▼
Change Sequence
```

The location dimension is therefore stored as **SCD Type 2**.

---

# Step 9 — Create Fact Table

The fact table also reads continuously from the Silver OBT.

```python
@dp.view
def fact_view():

    df = spark.readStream.table("silver_obt")

    df = df.select(
        "ride_id",
        "pickup_city_id",
        "payment_method_id",
        "driver_id",
        "passenger_id",
        "vehicle_id",
        "distance_miles",
        "duration_minutes",
        "base_fare",
        "distance_fare",
        "time_fare",
        "surge_multiplier",
        "total_fare",
        "tip_amount",
        "rating",
        "base_rate",
        "per_mile",
        "per_minute"
    )

    return df
```

Create the fact streaming table:

```python
dp.create_streaming_table("fact")
```

Create the Auto CDC flow:

```python
dp.create_auto_cdc_flow(
    target="fact",
    source="fact_view",
    keys=[
        "ride_id",
        "pickup_city_id",
        "payment_method_id",
        "driver_id",
        "passenger_id",
        "vehicle_id"
    ],
    sequence_by="ride_id",
    stored_as_scd_type=1,
)
```

---

# Step 10 — Complete Dimension and Fact Flow

```text
                         silver_obt
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
          ▼                  ▼                  ▼
   Passenger View       Driver View       Vehicle View
          │                  │                  │
          ▼                  ▼                  ▼
      Auto CDC            Auto CDC            Auto CDC
          │                  │                  │
          ▼                  ▼                  ▼
   dim_passenger       dim_driver        dim_vehicle


                         silver_obt
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
          ▼                  ▼                  ▼
    Payment View       Booking View       Location View
          │                  │                  │
          ▼                  ▼                  ▼
      Auto CDC            Auto CDC            Auto CDC
          │                  │                  │
          ▼                  ▼                  ▼
     dim_payment       dim_booking       dim_location


                         silver_obt
                             │
                             ▼
                         fact_view
                             │
                             ▼
                         Auto CDC
                             │
                             ▼
                           fact
```

---

# Step 11 — Final Star Schema

The dimensions and fact table form the analytical model.

```text
                     dim_passenger
                           │
                           │
dim_driver ────────┐       │       ┌──────── dim_vehicle
                   │       │       │
                   ▼       ▼       ▼
                 ┌─────────────────────┐
                 │        fact         │
                 │       rides         │
                 └─────────────────────┘
                   ▲       ▲       ▲
                   │       │       │
          dim_payment   dim_booking   dim_location
```

The fact table contains ride-level measurements, while the dimension tables contain descriptive information about the rides.

---

# Summary

In this phase:

- Streaming views are created from `silver_obt`.
- Streaming tables are created as targets.
- Auto CDC flows connect the views to the target tables.
- Passenger, driver, vehicle, payment, booking, and location dimensions are created.
- A fact table is created from the Silver OBT.
- `city_updated_at` is used for the location dimension's change sequence.
- The location dimension is stored as SCD Type 2.
- The dimensions and fact table form the foundation of the analytical star schema.

# website live 
https://uber-data-engineering-platform.onrender.com/