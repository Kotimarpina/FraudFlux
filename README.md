# FraudFlux
### Real-Time Financial Fraud Detection & Streaming Data Engineering Pipeline

**Stream. Detect. Protect.**

FraudFlux is an end-to-end streaming data engineering project designed to process financial transactions, enrich incoming events with customer information, identify potentially fraudulent activities, and support near-real-time monitoring.

The project demonstrates how modern data engineering technologies can combine streaming and historical data within a unified lakehouse architecture.

Built around **Apache Kafka, Apache Spark Structured Streaming, Databricks, PostgreSQL, Delta Lake, and Databricks Lakeflow**, FraudFlux explores scalable data ingestion, transformation, streaming joins, event-time processing, fraud detection, automated alerts, and pipeline orchestration.

---

## 1. Project Overview

Financial institutions continuously generate transaction data through payment cards, banking applications, and digital payment platforms.

Traditional batch-oriented systems may introduce delays when identifying suspicious transactions. Fraud monitoring systems therefore benefit from continuously processing incoming events and combining them with existing customer and fraud intelligence data.

FraudFlux demonstrates a streaming-oriented approach to this problem.

The pipeline integrates three principal data sources:

- **Live financial transactions:** Generated and published to Apache Kafka.
- **Customer reference information:** Retrieved from PostgreSQL.
- **Fraud watchlist information:** Ingested from streaming JSON files.

These datasets are processed using Databricks and Spark Structured Streaming to support transaction validation, customer enrichment, fraud monitoring, and analytical reporting.

## 2. Problem Statement

Financial transaction monitoring introduces several engineering challenges:

- Processing continuously arriving transaction events.
- Integrating streaming data with historical customer records.
- Handling structured and semi-structured data.
- Detecting transactions that violate customer-defined limits.
- Matching transaction events against changing fraud watchlists.
- Handling late-arriving events and maintaining streaming state.
- Generating alerts for suspicious activities.
- Providing updated information through analytical dashboards.

**Objective:** Demonstrate an end-to-end data engineering pipeline that integrates streaming and reference datasets to support fraud detection and operational analytics.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Transaction generation and supporting processing logic |
| Apache Kafka / Confluent Cloud | Event streaming and transaction ingestion |
| Apache Spark Structured Streaming | Continuous data processing |
| PySpark | Distributed data transformations |
| Databricks | Data engineering and lakehouse platform |
| PostgreSQL | Customer reference data storage |
| Lakeflow Connect | Ingestion of PostgreSQL source data |
| Lakeflow Spark Declarative Pipelines | Pipeline transformations and processing |
| Databricks Auto Loader | Incremental JSON file ingestion |
| Delta Lake | Structured lakehouse storage |
| SQL | Data querying and transformations |
| SMTP | Email-based fraud notifications |
| Lakeflow Jobs | Pipeline orchestration |
| Databricks Dashboards | Near-real-time monitoring |

## 4. System Architecture

The project uses multiple ingestion paths to combine streaming and reference data.

```text
       FINANCIAL TRANSACTION SOURCES
                    |
             Python Producer
                    |
             Confluent Kafka
                    |
          Spark Structured Streaming
                    |
                    v
            BRONZE DATA LAYER
                    |
          Data Cleaning & Parsing
                    |
                    v
            SILVER DATA LAYER
                    |
       +------------+------------+
       |                         |
       v                         v
 PostgreSQL Customer       JSON Fraud Watchlist
       |                         |
 Lakeflow Connect           Auto Loader
       |                         |
       v                         v
 Customer Reference       Watchlist Stream
       |                         |
       +------------+------------+
                    |
                    v
             FRAUD ANALYSIS
                    |
       +------------+------------+
       |                         |
  Stream-Static Join       Stream-Stream Join
       |                         |
  Customer Limit Check     Watchlist Matching
       |                         |
       +------------+------------+
                    |
                    v
              GOLD DATA LAYER
                    |
       +------------+------------+
       |                         |
   Fraud Alerts            Aggregations
       |                         |
   SMTP Email               Dashboards
                    |
             Lakeflow Jobs
              Orchestration
```

The architecture follows a **Bronze–Silver–Gold Medallion pattern** to separate raw ingestion, data refinement, and business-level processing.

## 5. Data Pipeline Workflow

### Stage 1: Transaction Generation and Kafka Ingestion

The pipeline begins with a Python-based transaction producer.

The producer generates transaction events and publishes them to a Kafka topic hosted through Confluent Cloud.

Apache Kafka provides the event-streaming infrastructure needed to decouple data producers from downstream processing systems.

**Key concepts:**
- Kafka producers and consumers
- Topics and partitions
- Message offsets
- Event serialization
- Continuous data ingestion

### Stage 2: Spark Structured Streaming

Databricks connects to Kafka using Spark Structured Streaming.

Incoming transaction events are read continuously and prepared for downstream transformations.

The processing workflow introduces:

- Streaming DataFrames
- Kafka source connections
- Streaming triggers
- Checkpointing concepts
- Incremental event processing

This stage establishes the streaming ingestion foundation of the project.

### Stage 3: Bronze Layer – Raw Data Ingestion

The Bronze layer represents the initial landing zone for source data.

Raw transaction events are ingested from Kafka, while other source datasets are introduced through their respective ingestion mechanisms.

**Purpose:**
- Preserve incoming source information.
- Support subsequent transformations.
- Separate ingestion from business logic.
- Maintain a foundation for downstream processing.

### Stage 4: Silver Layer – Data Transformation

The Silver layer prepares datasets for downstream analytical operations.

Processing activities include:

- Parsing incoming JSON records.
- Extracting transaction attributes.
- Applying appropriate data types.
- Standardizing data structures.
- Preparing customer and transaction datasets.
- Organizing fraud watchlist information.

The objective is to produce consistent, usable datasets for streaming joins and fraud evaluation.

### Stage 5: PostgreSQL Customer Data Integration

Customer reference information is stored in PostgreSQL.

The tutorial demonstrates ingestion of PostgreSQL data into Databricks through Lakeflow Connect.

This information is used to enrich transaction events with customer-level attributes.

**Engineering concepts demonstrated:**
- Relational database integration
- Historical/reference data ingestion
- SQL-based data access
- Integration of streaming and non-streaming datasets

### Stage 6: Stream-Static Join

A stream-static join combines continuously arriving transaction events with a relatively static customer reference dataset.

The pipeline uses customer information to evaluate transaction amounts against configured limits.

**Illustrative business rule:**

```text
IF transaction_amount > customer_limit:
    Flag transaction for review
ELSE:
    Continue normal processing
```

This demonstrates how streaming systems can use existing customer information to evaluate live transactions.

### Stage 7: Fraud Watchlist Ingestion

The project also introduces a changing fraud watchlist.

Watchlist data arrives through JSON files and is ingested using Databricks Auto Loader.

The ingestion process demonstrates incremental file discovery and processing.

The resulting watchlist stream is prepared for correlation with incoming financial transactions.

### Stage 8: Stream-Stream Join

Unlike stream-static joins, stream-stream joins combine two continuously changing datasets.

In FraudFlux, transaction events are correlated with fraud watchlist updates.

This demonstrates:

- Correlating multiple event streams.
- Maintaining streaming state.
- Using event-time constraints.
- Matching transactions with watchlist information.

This capability is particularly relevant when fraud intelligence changes while transactions continue to arrive.

### Stage 9: Stateful Processing and Watermarks

Streaming applications must manage records that arrive out of order or later than expected.

The tutorial introduces watermarks and stateful processing.

**Stateless processing** evaluates records without retaining information from earlier events.

**Stateful processing** maintains information across events, supporting operations such as streaming joins and aggregations.

Watermarks help manage event-time processing and control how long streaming state must be retained.

### Stage 10: Window-Based Aggregations

The project demonstrates time-windowed aggregation of streaming data.

Window-based operations can support:

- Transaction counts over defined periods.
- Transaction volume analysis.
- Monitoring changes in activity.
- Summarizing streaming events.

The tutorial discusses windowing concepts and their application to continuously arriving records.

### Stage 11: Fraud Alert Generation

Fraud detection results can trigger automated notifications.

The tutorial demonstrates email-based alerts using SMTP.

The alerting workflow is intended to notify relevant recipients when transactions match configured suspicious-activity conditions.

**Security consideration:** SMTP credentials, Kafka credentials, database passwords, and access tokens must never be committed to a public repository.

### Stage 12: Near-Real-Time Dashboard

Processed transaction information is presented through a Databricks dashboard.

The dashboard provides a monitoring interface for updated analytical results.

This stage demonstrates how a streaming data pipeline can support downstream business intelligence and operational visibility.

### Stage 13: Workflow Orchestration

The final stage uses Databricks Lakeflow Jobs to orchestrate the end-to-end workflow.

Orchestration helps organize pipeline execution and dependencies between processing stages.

This brings together ingestion, transformation, fraud evaluation, and reporting into a coordinated data engineering solution.

---

## 6. Core Data Engineering Concepts

FraudFlux demonstrates several important concepts:

| Concept | Application |
|---|---|
| Event streaming | Kafka-based transaction ingestion |
| Distributed processing | Apache Spark and PySpark |
| Streaming pipelines | Spark Structured Streaming |
| Batch/reference integration | PostgreSQL and Lakeflow Connect |
| Incremental file ingestion | Databricks Auto Loader |
| Data lakehouse | Delta Lake and Databricks |
| Medallion architecture | Bronze, Silver, Gold processing |
| Stream-static joins | Customer reference enrichment |
| Stream-stream joins | Fraud watchlist matching |
| Stateful processing | Maintaining streaming context |
| Watermarks | Managing late-arriving events |
| Window aggregations | Time-based transaction analytics |
| Orchestration | Databricks Lakeflow Jobs |

## 7. Repository Structure

The original tutorial repository organizes the implementation into the following major components.

```text
FraudFlux/
|
|-- dashboard/
|   |-- Dashboard-related files
|
|-- databricks notebooks and pipelines/
|   |-- finguard_project/
|       |-- Databricks notebooks
|       |-- Streaming pipeline resources
|
|-- kafka_producer/
|   |-- Kafka producer implementation
|
|-- postgres sql/
|   |-- PostgreSQL-related SQL files
|
|-- README.md
```

The directory structure above summarizes the verified top-level source layout. Individual file names and notebook paths should be checked against the actual FraudFlux repository.

## 8. Setup Requirements

The tutorial uses the following services and tools:

1. Databricks workspace with access to the required streaming and Lakeflow features.
2. Confluent Cloud account and Kafka topic.
3. PostgreSQL database.
4. Python environment for the Kafka producer.
5. Appropriate service credentials and permissions.
6. SMTP configuration for email alerts.

### Recommended execution order

1. Configure Confluent Kafka and the transaction producer.
2. Establish the Spark-to-Kafka connection.
3. Configure Databricks credentials securely.
4. Create and run transaction ingestion pipelines.
5. Prepare PostgreSQL customer reference data.
6. Ingest customer information into Databricks.
7. Implement the stream-static fraud checks.
8. Configure JSON watchlist ingestion.
9. Implement stream-stream watchlist matching.
10. Configure streaming aggregations and alerts.
11. Create the monitoring dashboard.
12. Orchestrate the complete workflow using Lakeflow Jobs.

For executable configuration and commands, follow the original source notebooks and the tutorial. Exact setup steps depend on the Databricks workspace, service configuration, and repository files.

## 9. Project Demonstration – STAR Framework

### Situation

Financial transaction monitoring requires processing continuously arriving events while integrating customer reference data and changing fraud intelligence.

### Task

Develop and understand an end-to-end streaming pipeline capable of ingesting, transforming, enriching, and evaluating financial transactions.

### Action

The tutorial implementation demonstrates Kafka-based ingestion, Spark Structured Streaming transformations, PostgreSQL reference integration, fraud watchlist correlation, event-time processing, and automated monitoring workflows.

### Result

The resulting architecture demonstrates how streaming and reference data can be integrated into a lakehouse-based fraud monitoring workflow.

*Personal implementation results, successful test runs, and performance measurements should be documented separately after verification.*

## 10. Project Impact – SBI Framework

**Situation:** Financial monitoring systems need timely access to transaction events and supporting customer information.

**Behavior:** The architecture combines streaming ingestion, data transformation, streaming joins, fraud evaluation, and orchestration.

**Impact:** The solution demonstrates an approach for producing fraud-related signals and operational analytics from continuously arriving data.

## 11. Challenges and Learning Outcomes

This project is useful for understanding:

- The difference between batch and streaming data processing.
- How Kafka integrates with Spark.
- Why streaming systems require checkpoints.
- How streaming joins differ from traditional SQL joins.
- How event-time windows and watermarks work.
- How reference data can enrich streaming transactions.
- Why layered data architectures improve data organization.
- How pipeline orchestration supports end-to-end workflows.

## 12. Future Enhancements

Potential extensions include:

- Implementing configurable fraud detection rules.
- Adding automated data quality checks.
- Introducing schema validation and schema evolution handling.
- Monitoring pipeline throughput and processing latency.
- Adding unit tests for transformation logic.
- Developing a fraud investigation dashboard.
- Introducing CI/CD for pipeline deployment.
- Implementing additional fraud risk indicators.

These are proposed improvements, not features claimed as completed.

## 13. Security and Responsible Usage

This repository is intended for educational and portfolio demonstration purposes.

- Use synthetic or appropriately authorized data.
- Never expose real financial information.
- Store credentials securely.
- Avoid committing secrets or environment files.
- Review access permissions before deploying services.
- Do not treat demonstration fraud rules as production-ready financial controls.

## 14. Project Reference and Attribution

FraudFlux is an educational adaptation of the **FinGuard streaming data engineering project** presented by DataBeli.

**Original creator:** DataBeli / Narender Kumar

**YouTube tutorial:**
https://www.youtube.com/watch?v=SsE5ZfxtiOI

**Original source repository:**
https://github.com/databeli/finguard_streaming_project

Credit for the original architecture, instructional materials, and source implementation belongs to the original creator.

This repository uses the name FraudFlux for the educational portfolio adaptation. Any independent modifications and enhancements should be documented in the repository's commit history and project documentation.

Please refer to the original repository for applicable licensing and reuse conditions.

---

## 15. Conclusion

FraudFlux demonstrates an end-to-end streaming data engineering architecture combining Apache Kafka, Spark Structured Streaming, Databricks, PostgreSQL, and modern lakehouse concepts.

The project provides practical exposure to event ingestion, data transformation, streaming joins, state management, fraud monitoring, and workflow orchestration.

It serves as a learning-focused implementation of real-time data engineering concepts relevant to financial analytics and scalable data platforms.

**FraudFlux — Stream. Detect. Protect.**
