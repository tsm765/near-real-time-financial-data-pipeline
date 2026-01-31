# Near-Real-Time Financial Analytics Pipeline

## Overview
This project demonstrates an end-to-end data engineering pipeline that ingests financial price data and market attention signals, processes them in near-real-time, and makes them available for analytics and visualization.

## Architecture
Attached in files section

## Tech Stack
- Python
- Apache Kafka
- Docker & Docker Compose
- Azure Data Lake Storage Gen2
- Azure Synapse Analytics (Serverless SQL)
- Power BI

## Data Flow
1. External APIs provide price and news mention data.
2. Dockerized Kafka producers publish events to Kafka topics.
3. A Kafka consumer processes events and writes partitioned Parquet files to Azure Data Lake.
4. Azure Synapse SQL views transform data using a Bronze → Silver → Gold approach.
5. Power BI connects to curated views for analytics and dashboards.

## What This Project Demonstrates
- Event-driven data ingestion
- Streaming-friendly data modeling
- Cloud-based analytics engineering
- Separation of raw, cleaned, and analytics-ready data

## Out of Scope
- No predictive modeling or trading signals
- No real-time execution systems

## Screenshots
Attached in files section
