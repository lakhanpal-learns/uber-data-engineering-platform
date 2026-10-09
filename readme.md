# **UBER REAL-TIME DATA ENGINEERING PROJECT**

An end-to-end data engineering project inspired by a ride-hailing platform, combining historical batch ingestion and real-time ride events to build an analytics-ready data platform using Microsoft Azure, Databricks, PySpark, and modern data engineering technologies.

## Project Overview

This project demonstrates how historical and real-time ride data can be ingested, processed, transformed, and prepared for analytics.

The platform combines Azure Data Factory for historical data ingestion, Azure Event Hubs for real-time event streaming, and Databricks for data processing and transformation. It follows a Bronze and Silver data-layer approach, with CDC-based fact and dimension modeling supporting downstream analytics.

The project also integrates Databricks SQL Warehouse with Power BI and an external Flask web dashboard.

**Project website:** https://uber-data-engineering-platform.onrender.com/

**Tutorial reference:** [Ansh Lab — Project Tutorial](https://youtu.be/5KIbhHo6GJA?si=_glK4yZIYfTzQ1cQ)

## Architecture

```text
Historical Data                  Real-Time Ride Events
      │                                  │
    GitHub                            Web App
      │                                  │
      ▼                                  ▼
Azure Data Factory                Azure Event Hubs
      │                                  │
      ▼                                  │
Azure Data Lake Gen2                      │
      │                                  │
      └──────────────┬───────────────────┘
                     ▼
                 Databricks
                     │
                     ▼
              Bronze Layer
                     │
                  stg_rides
                     │
                     ▼
               Silver Layer
                silver_obt
                     │
                     ▼
                  Auto CDC
                     │
             ┌───────┴───────┐
             ▼               ▼
        Dimensions          Fact
             └───────┬───────┘
                     ▼
                Star Schema
                     │
                     ▼
          Databricks SQL Warehouse
                 /          \
                ▼            ▼
            Power BI     Flask API
                              │
                              ▼
                        Web Dashboard
```

## Technology Stack

- **Cloud:** Microsoft Azure
- **Batch ingestion:** Azure Data Factory
- **Event streaming:** Azure Event Hubs
- **Cloud storage:** Azure Data Lake Storage Gen2
- **Processing:** Databricks, PySpark, Spark Structured Streaming
- **Storage format:** Delta Lake
- **Pipeline development:** Lakeflow Declarative Pipelines
- **Transformations:** SQL, Jinja, metadata-driven configuration
- **Change processing:** Auto CDC, SCD Type 1 and Type 2
- **Data modeling:** Fact tables, dimension tables, star schema
- **Governance:** Unity Catalog
- **Analytics:** Databricks SQL Warehouse, Power BI
- **Web application:** Flask, HTML, CSS, JavaScript
- **Deployment:** Render
- **Version control:** Git and GitHub

## Key Features

### Batch and Real-Time Ingestion
- Ingest historical ride data using Azure Data Factory and Azure Data Lake Gen2.
- Stream ride events through Azure Event Hubs.
- Process real-time events with Spark Structured Streaming.
- Combine historical and streaming ride data in the `stg_rides` staging table.

### Bronze and Silver Processing
- Store and organize incoming ride data and mapping datasets.
- Build the enriched `silver_obt` table.
- Join vehicle, payment, ride status, city, and cancellation reference data.
- Use metadata-driven SQL generation with Jinja.
- Apply streaming watermarks and manage schema changes.

### CDC and Dimensional Modeling
- Create fact and dimension datasets from enriched ride data.
- Use Auto CDC flows for change processing.
- Apply SCD Type 1 patterns to selected dimensions and an SCD Type 2 pattern to the location dimension.
- Structure analytical data around a star-schema approach.

### Analytics and Dashboard Integration
- Query analytical tables using Databricks SQL Warehouse.
- Connect a Flask backend to Databricks using the Databricks SQL Connector.
- Expose ride analytics through REST API endpoints.
- Display KPIs and analytical breakdowns through a web dashboard.
- Deploy the external dashboard on Render.

## Learning Outcomes

This project provided hands-on learning in:

- Batch versus real-time data ingestion.
- Azure Event Hubs and Kafka-compatible streaming.
- Spark Structured Streaming, checkpoints, and watermarks.
- Bronze and Silver data architecture.
- Metadata-driven data transformations.
- Delta Lake and schema evolution.
- Change Data Capture and SCD concepts.
- Fact and dimension modeling.
- Unity Catalog and Databricks SQL Warehouse.
- Connecting data platforms to external applications.
- Cloud deployment and version control.

## Acknowledgement

This project was developed as a hands-on learning project using **Ansh Lab's tutorial** as a reference.

The tutorial provided the starting point for understanding the architecture and technologies. I worked through the implementation, explored the concepts, made modifications, and developed the external dashboard and deployment as part of my learning journey.

Special thanks to Ansh Lab for sharing this practical Data Engineering project.

**Tutorial:** https://youtu.be/5KIbhHo6GJA?si=_glK4yZIYfTzQ1cQ

## Disclaimer

This is an independent educational project inspired by a ride-hailing use case. It is not an official Uber product and is not affiliated with Uber.

---

*Built as part of my journey toward becoming a Data Engineer.*

**Learn • Build • Improve**




