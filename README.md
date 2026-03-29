# E-Commerce Data Pipeline

A production-grade e-commerce data pipeline built with **Scala**, **Apache Spark**, and **Apache Kafka**.

## Architecture

```
[MockDataGenerator] → [KafkaProducer] → [Kafka Topics] → [SparkStreamingConsumer]
                                                                    ↓
                                                        [DataCleaner → DataTransformer]
                                                                    ↓
                                                     [PostgresWriter + ParquetWriter]
                                                                    ↓
                                                    [PostgreSQL DB + Parquet Data Lake]
```

## Components

| Component | Description |
|-----------|-------------|
| **MockDataGenerator** | Generates realistic e-commerce transactions, clients, and products |
| **KafkaProducer** | Publishes generated data to Kafka topics |
| **SparkStreamingConsumer** | Reads from Kafka using Spark Structured Streaming |
| **DataCleaner** | Validates, deduplicates, and normalizes raw data |
| **DataTransformer** | Computes KPIs (revenue by category/country, top products, segments) |
| **PostgresWriter** | Writes cleaned data and KPIs to PostgreSQL |
| **ParquetWriter** | Writes data to partitioned Parquet files |
| **PipelineOrchestrator** | Coordinates all components, health checks, demo mode |

## Infrastructure

- **Apache Kafka** (Confluent 7.6) — message broker with 3 topics
- **Apache Zookeeper** — Kafka coordination
- **Kafka UI** — web UI at http://localhost:8080
- **PostgreSQL 16** — relational storage

## Quick Start

```bash
# Start infrastructure
docker compose up -d

# Compile
sbt compile

# Run demo (produces data, processes, stores, generates report)
sbt "runMain orchestration.PipelineOrchestrator --mode demo"

# Build fat JAR
sbt assembly
```

## Kafka Topics

| Topic | Description |
|-------|-------------|
| `transactions` | Purchase transactions |
| `clients` | Customer profiles |
| `products` | Product catalog |

## Output

- **PostgreSQL**: `ecommerce` database with raw tables and KPI tables
- **Parquet**: `output/parquet/` partitioned by date
- **Reports**: `output/reports/` run summaries

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `POSTGRES_URL` | `jdbc:postgresql://localhost:5432/ecommerce` | PostgreSQL JDBC URL |
| `POSTGRES_USER` | `pipeline` | Database user |
| `POSTGRES_PASSWORD` | `pipeline123` | Database password |
