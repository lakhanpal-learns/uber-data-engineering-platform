# Phase 00 — Project Introduction

## 1. Introduction

The **Uber-Like Data Platform** is an end-to-end data engineering project designed to simulate how a large ride-booking platform can handle both **historical/batch data** and **real-time streaming data**.

A platform like Uber generates data from many different sources, including:

- Customers booking rides
- Drivers accepting rides
- Ride status changes
- Pickup and drop-off locations
- Payments
- Trip details
- Driver information
- Customer information

The main challenge is to build a data platform that can handle both large amounts of historical data and continuously arriving real-time events.

The platform therefore needs to support:

- Historical data ingestion
- Real-time event ingestion
- Scalable data processing
- Data transformation and enrichment
- Analytical data modeling
- Business analytics and reporting

---

# 2. Complete Project Workflow

The overall architecture of the project is:

```text
                        UBER-LIKE DATA PLATFORM
                                  │
                  ┌───────────────┴───────────────┐
                  │                               │
           HISTORICAL DATA                  REAL-TIME DATA
                  │                               │
             GitHub / API                       Web App
                  │                               │
                  ▼                               ▼
          Azure Data Factory                Azure Event Hub
                  │                               │
                  ▼                               ▼
           Azure Data Lake                    Databricks
                  │                               │
                  └───────────────┬───────────────┘
                                  │
                                  ▼
                              Databricks
                                  │
                         Bronze → Silver → Gold
                                  │
                                  ▼
                             Star Schema
                                  │
                                  ▼
                              Analytics
```

The project contains two major data pipelines:

1. **Historical / Batch Data Pipeline**
2. **Real-Time Streaming Data Pipeline**

Both pipelines eventually converge in Databricks, where the data is processed through the **Bronze → Silver → Gold** architecture.

---

# 3. Real-Time Streaming Data

## 3.1 Web Application

The project contains a web application that simulates a ride-booking platform.

The application generates ride-related events such as:

- Ride booking
- Driver acceptance
- Ride status changes
- Pickup events
- Drop-off events
- Payment events
- Trip information

These events are generated continuously as users interact with the application.

The simplified flow is:

```text
Web Application
       │
       │ Ride Events
       ▼
Azure Event Hub
```

---

## 3.2 Azure Event Hub

**Azure Event Hub** acts as the real-time event ingestion layer.

The web application produces events, and Event Hub receives and stores these events temporarily so that downstream systems can process them.

```text
Web App
   │
   │ Events
   ▼
Event Hub
   │
   │ Real-Time Stream
   ▼
Databricks
```

Databricks consumes the events from Event Hub using streaming technologies such as **Spark Structured Streaming**.

---

# 4. Historical / Batch Data

## 4.1 Problem

A large ride-booking company generates a huge amount of historical data.

This data may contain information about:

- Previous rides
- Customers
- Drivers
- Vehicles
- Payments
- Locations
- Ride statuses
- Historical trip information

The company needs a system capable of ingesting and processing this large amount of historical data.

---

## 4.2 Real-World Concept

A simplified representation of how an organization could move historical data into a data platform is:

```text
Internal Data Sources
        ↓
     Internal APIs
        ↓
Data Engineering Pipeline
        ↓
      Data Lake
```

In this project, we do not have access to Uber's internal systems.

Therefore, **GitHub and publicly accessible APIs are used as a mock/source system for historical data**.

The batch pipeline is:

```text
GitHub
   ↓
GitHub API
   ↓
Azure Data Factory
   ↓
Azure Data Lake
   ↓
Databricks
```

---

# 5. Batch Processing Workflow

The historical data follows this general workflow:

```text
Historical Data
      ↓
    Ingest
      ↓
    Store
      ↓
   Process
      ↓
 Transform
      ↓
   Analyze
```

Azure Data Factory is responsible for orchestrating the historical data ingestion process.

The data is then stored in Azure Data Lake before being processed by Databricks.

---

# 6. Real-Time Processing Workflow

The real-time data follows a different ingestion path:

```text
Web Application
      ↓
Azure Event Hub
      ↓
Databricks / Spark Streaming
      ↓
Bronze
      ↓
Silver
      ↓
Gold
      ↓
Analytics
```

Unlike batch processing, the streaming pipeline continuously processes new events as they arrive.

---

# 7. Unified Data Platform

The main goal of this architecture is to bring both historical and real-time data into a single data platform.

```text
                     DATA SOURCES
                          │
             ┌────────────┴────────────┐
             │                         │
             ▼                         ▼
      Historical Data            Real-Time Events
             │                         │
             ▼                         ▼
      Azure Data Factory          Azure Event Hub
             │                         │
             ▼                         ▼
       Azure Data Lake             Databricks
             │                         │
             └────────────┬────────────┘
                          │
                          ▼
                      Databricks
                          │
                          ▼
                       Bronze
                          │
                          ▼
                       Silver
                          │
                          ▼
                        Gold
                          │
                          ▼
                     Star Schema
                          │
                          ▼
                      Analytics
```

This allows the platform to process:

- Large historical datasets
- Continuously arriving real-time events

within the same downstream processing architecture.

---

# 8. Medallion Architecture

The Databricks processing layer follows the **Bronze → Silver → Gold** architecture.

## 8.1 Bronze Layer

The Bronze layer contains the raw or minimally processed data coming from the source systems.

```text
Data Sources
     ↓
  Bronze
```

The main sources are:

- Azure Data Lake
- Azure Event Hub

The purpose of this layer is to preserve the incoming data before applying extensive transformations.

---

## 8.2 Silver Layer

The Silver layer contains cleaned, standardized, and enriched data.

```text
Bronze
   ↓
Cleaning
   ↓
Transformation
   ↓
Enrichment
   ↓
Silver
```

Typical processing includes:

- Data cleaning
- Data type conversion
- Deduplication
- Data validation
- Joining reference data
- Data enrichment
- Standardization

---

## 8.3 Gold Layer

The Gold layer contains business-ready and analytics-ready data.

```text
Silver
   ↓
Business Transformations
   ↓
Gold
```

The Gold layer is then used to create analytical models such as a **Star Schema**.

---

# 9. Star Schema

The final analytical data model follows a Star Schema design.

A simplified representation is:

```text
                    Dim Passenger
                          │
                          │
Dim Driver ─────────── Fact Ride ─────────── Dim Vehicle
                          │
                          │
                    Dim Payment
                          │
                          │
                     Dim Location
                          │
                          │
                     Dim Booking
```

The **Fact Ride** table contains measurable ride-related information.

Dimension tables contain descriptive information about entities such as:

- Passengers
- Drivers
- Vehicles
- Payments
- Locations
- Bookings

This structure makes the data suitable for analytical queries and BI reporting.

---

# 10. Business Problem → Technical Solution

| Problem | Project Solution |
|---|---|
| Large amount of historical data | GitHub → API → Azure Data Factory → Azure Data Lake |
| Continuous incoming ride events | Web App → Azure Event Hub |
| Automated data ingestion | Azure Data Factory |
| Real-time data processing | Databricks + Spark Structured Streaming |
| Scalable data processing | Databricks / Apache Spark |
| Raw data storage and processing | Bronze Layer |
| Clean and enriched data | Silver Layer |
| Analytics-ready data | Gold Layer |
| Analytical data modeling | Star Schema |
| Business reporting and analysis | Analytics / BI Layer |

---

# 11. Technology Stack

| Technology | Purpose |
|---|---|
| GitHub / API | Historical data source |
| Azure Data Factory | Batch ingestion and orchestration |
| Azure Data Lake | Historical data storage |
| Azure Event Hub | Real-time event ingestion |
| Databricks | Data processing platform |
| Apache Spark | Distributed data processing |
| Spark Structured Streaming | Real-time stream processing |
| Bronze / Silver / Gold | Data transformation architecture |
| Star Schema | Analytical data modeling |
| BI / Analytics | Reporting and business analysis |

---

# 12. End-to-End Architecture

The complete end-to-end workflow can be summarized as:

```text
                         DATA SOURCES
                              │
               ┌──────────────┴──────────────┐
               │                             │
               ▼                             ▼
        HISTORICAL DATA                REAL-TIME DATA
               │                             │
          GitHub / API                   Web App
               │                             │
               ▼                             ▼
      Azure Data Factory              Azure Event Hub
               │                             │
               ▼                             ▼
       Azure Data Lake                 Databricks
               │                             │
               └──────────────┬──────────────┘
                              │
                              ▼
                           BRONZE
                              │
                              ▼
                           SILVER
                              │
                              ▼
                             GOLD
                              │
                              ▼
                        STAR SCHEMA
                              │
                              ▼
                          ANALYTICS
``` 

---

# 13. Project Objectives

The project aims to demonstrate the following data engineering capabilities:

- Designing a cloud-based data platform
- Handling batch and streaming data
- Building automated data ingestion pipelines
- Working with Azure Data Factory
- Working with Azure Data Lake
- Working with Azure Event Hub
- Processing data using Databricks and Apache Spark
- Implementing Bronze, Silver, and Gold layers
- Building analytical data models
- Implementing fact and dimension tables
- Supporting downstream analytics and BI

---



