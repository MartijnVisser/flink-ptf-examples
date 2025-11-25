# Apache Flink ProcessTableFunction Examples

This repository contains example implementations of Apache Flink ProcessTableFunctions (PTFs) that connect to Confluent Cloud Kafka clusters using Avro Confluent format.

## Overview

This project demonstrates how to:
- Create custom ProcessTableFunctions in Apache Flink 2.1
- Connect to Confluent Cloud Kafka using the Kafka connector 2.0.1
- Use Avro Confluent format for deserialization
- Run Flink SQL queries using PTFs
- Deploy everything in a Docker environment

## Example: Hybrid Window PTF

The included `HybridWindowFunction` implements a hybrid tumbling window that:
- Creates time-based windows (e.g., 5 second windows) aligned to epoch boundaries
- Emits window results early if the order count exceeds a threshold
- Aggregates orders and calculates total price per window
- Supports partitioning by key (e.g., customer_id)
- Uses Flink's time context for accurate window boundaries

**Important Behavior**: When the count threshold triggers an early emission, the state resets and a new aggregation begins. If more events arrive within the same wall-clock tumbling window period, they will be aggregated into a new window with the **same `window_start` and `window_end` timestamps** but different aggregated values. This means multiple rows may share identical window boundaries - this is by design and allows continuous windowing per partition.

## Prerequisites

- Docker and Docker Compose installed
- Maven 3.6+ (for building the project)
- Java 17+
- Confluent Cloud account with:
  - Kafka cluster
  - Schema Registry
  - API keys and secrets

## Setup

### 1. Build the Project

Build the Maven project to create the PTF JAR:

```bash
mvn clean package
```

This will:
- Compile the Java code
- Package the PTF classes into a JAR file
- Place the JAR in `target/flink-ptf-examples-1.0.1.jar`

### 2. Build Docker Image

Build the Docker image that includes Flink, Kafka connector, Avro format, and the PTF:

```bash
docker-compose build --no-cache
```

This Dockerfile automatically:
- Uses Flink 2.1.0 base image
- Downloads and installs Flink Kafka connector 2.0.1
- Downloads and installs Flink SQL Avro Confluent format
- Includes the PTF JAR in the Flink lib directory

## Running the Examples

### Start Docker Containers

Start the Flink cluster:

```bash
docker-compose up -d
```

This starts:
- **JobManager**: Flink cluster manager (port 8081)
- **TaskManager**: Flink worker nodes
- **SQL Client**: Flink SQL client container (for interactive queries)

### Access Flink Web UI

Open your browser and navigate to:
```
http://localhost:8081
```

You can monitor jobs, check task manager status, and view job execution details.

### Run Flink SQL Client

```bash
docker compose run --rm sql-client
```

### Execute Example SQL Queries

#### Prerequisite: Orders Data in Confluent Cloud

This example assumes you have an `orders` topic with data in Confluent Cloud. If you don't have one, you can easily create it using [Confluent Cloud for Apache Flink](https://confluent.cloud) by running the following SQL in the Confluent Cloud for Apache Flink workspace:

```sql
CREATE TABLE orders AS SELECT * FROM examples.marketplace.orders;
```

This creates an `orders` table populated with sample marketplace data from Confluent's built-in examples.

#### 1. Create Kafka Table

Create the Kafka table in your local Flink cluster to connect to your Confluent Cloud topic. Execute the following SQL in the Flink SQL Client:

```sql
CREATE TABLE orders (
    order_id VARCHAR(2147483647) NOT NULL,
    customer_id INT NOT NULL,
    product_id VARCHAR(2147483647) NOT NULL,
    price DOUBLE NOT NULL,
    ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'YOUR_BOOTSTRAP_SERVERS',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'PLAIN',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="YOUR_KAFKA_API_KEY" password="YOUR_KAFKA_API_SECRET";',
    'properties.group.id' = 'flink-ptf-consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'YOUR_SCHEMA_REGISTRY_URL',
    'avro-confluent.basic-auth.credentials-source' = 'USER_INFO',
    'avro-confluent.basic-auth.user-info' = 'YOUR_SCHEMA_REGISTRY_API_KEY:YOUR_SCHEMA_REGISTRY_API_SECRET'
);
```

#### 2. Register the PTF Function

Register the ProcessTableFunction by executing the following SQL in the Flink SQL Client:

```sql
CREATE FUNCTION HybridWindow AS 'com.flink.ptf.HybridWindowFunction';
```

#### 3. Use the PTF in a Query

Query using the hybrid window. Execute the following SQL in the Flink SQL Client:

```sql
SELECT 
    customer_id,
    window_start,
    window_end,
    num_orders,
    total_price
FROM HybridWindow(
    input => TABLE orders PARTITION BY customer_id,
    windowSizeMillis => CAST(5000 AS BIGINT),
    countThreshold => 5000,
    on_time => DESCRIPTOR(ts),
    uid => 'hybrid-win-v3'
);
```

**Important Note on `windowSizeMillis` Parameter**: Due to a known Flink issue ([FLINK-37618](https://issues.apache.org/jira/browse/FLINK-37618)), PTFs do not currently support `INTERVAL` arguments. You **must** use `CAST(5000 AS BIGINT)` instead of `INTERVAL '5' SECONDS` for the `windowSizeMillis` parameter. The value should be specified in milliseconds (e.g., `CAST(5000 AS BIGINT)` for 5 seconds).

This query:
- Partitions orders by `customer_id`
- Creates 5-second (5000ms) tumbling windows
- Emits early if more than 5000 orders are received in a window
- Aggregates orders and calculates total price per window
- Uses `ts` as the event time column

## Understanding the PTF

### HybridWindowFunction

The `HybridWindowFunction` class extends `ProcessTableFunction` and implements:

1. **Window State Management**: Tracks orders and aggregates data within each window partition
2. **Time-based Windowing**: Creates windows based on a duration in milliseconds (e.g., 5 seconds)
3. **Early Emission**: Emits window results when order count exceeds threshold
4. **Timer-based Expiration**: Uses Flink timers to emit windows at their end time
5. **Aggregation**: Calculates total price and order count per window

### Parameters

- `windowSizeMillis`: The duration of each tumbling window in milliseconds. **Must be specified as `CAST(5000 AS BIGINT)`** (not `INTERVAL '5' SECONDS`) due to [FLINK-37618](https://issues.apache.org/jira/browse/FLINK-37618). Example: `CAST(5000 AS BIGINT)` for 5 seconds.
- `countThreshold`: Maximum number of orders before early emission (e.g., `5000`)
- `on_time`: Event time descriptor (e.g., `DESCRIPTOR(ts)`)
- `uid`: Unique identifier for the PTF instance (required for stateful transformations)

### Output Schema

The PTF outputs:
- `window_start`: Start timestamp of the window (TIMESTAMP(3))
- `window_end`: End timestamp of the window (TIMESTAMP(3))
- `num_orders`: Number of orders in the window (INT)
- `total_price`: Total price of all orders in the window (DOUBLE)

## Stopping Docker Containers

To stop all containers:

```bash
docker-compose down
```

## Project Structure

```
flink-ptfs/
├── pom.xml                                    # Maven configuration
├── Dockerfile                                 # Docker image definition
├── docker-compose.yml                         # Docker Compose configuration
├── README.md                                  # This file
├── src/
│   └── main/
│       ├── java/
│       │   └── com/
│       │       └── flink/
│       │           └── ptf/
│       │               └── HybridWindowFunction.java  # PTF implementation
└── target/                                    # Build output (generated)
```
