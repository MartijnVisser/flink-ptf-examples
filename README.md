# Apache Flink ProcessTableFunction Examples

This repository contains example implementations of Apache Flink ProcessTableFunctions (PTFs) that connect to Confluent Cloud Kafka clusters using Avro Confluent format.

## Overview

This project demonstrates how to:
- Create custom ProcessTableFunctions in Apache Flink 2.1
- Connect to Confluent Cloud Kafka using the Kafka connector 2.0.1
- Use Avro Confluent format for deserialization
- Run Flink SQL queries using PTFs
- Deploy everything in a Docker environment

## Examples

### 1. Hybrid Window PTF

The `HybridWindowFunction` implements a hybrid tumbling window that:
- Creates time-based windows (e.g., 5 second windows) aligned to epoch boundaries
- Emits window results early if the order count exceeds a threshold
- Aggregates orders and calculates total price per window
- Supports partitioning by key (e.g., customer_id)
- Uses Flink's time context for accurate window boundaries

**Important Behavior**: When the count threshold triggers an early emission, the state resets and a new aggregation begins. If more events arrive within the same wall-clock tumbling window period, they will be aggregated into a new window with the **same `window_start` and `window_end` timestamps** but different aggregated values. This means multiple rows may share identical window boundaries - this is by design and allows continuous windowing per partition.

### 2. Early Fire Window PTF

The `EarlyFireWindowFunction` implements a tumbling window that emits on every incoming event:
- Creates time-based windows aligned to epoch boundaries (like standard tumbling windows)
- Emits the current window aggregation immediately on every event (no waiting for window close)
- No timers required - window transitions are detected by comparing event timestamps
- Aggregates orders and calculates total price per window
- Supports partitioning by key (e.g., customer_id)

**Important: Running Aggregates**

Each emission contains the **cumulative aggregation** of all events within the current window, not just the single triggering event. For example, if 3 events arrive for the same partition in a 5-second window:

| Event # | Event Time | Emitted `num_orders` | Emitted `total_price` |
|---------|------------|---------------------|----------------------|
| 1 | 10:00:01 | 1 | 10.0 |
| 2 | 10:00:02 | 2 | 25.0 *(running total)* |
| 3 | 10:00:03 | 3 | 40.0 *(running total)* |
| 4 | 10:00:06 | 1 *(new window, reset)* | 15.0 |

This provides progressive refinement of window results as data arrives.

**Key Differences from Hybrid Window**:

| Aspect | HybridWindowFunction | EarlyFireWindowFunction |
|--------|---------------------|------------------------|
| **Emission trigger** | Count threshold OR timer | Every event |
| **Timer usage** | Yes (window end timer) | No |
| **State reset** | On emission | On window boundary detection |
| **Parameters** | `windowSizeMillis`, `countThreshold` | `windowSizeMillis` only |

**Use Cases**:
- Real-time dashboards requiring immediate updates with running totals
- Alerting systems that need to react to partial aggregations
- Progressive result refinement as more data arrives

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

#### 2. Register the PTF Functions

Register the ProcessTableFunctions by executing the following SQL in the Flink SQL Client:

```sql
-- Hybrid Window PTF (emits on count threshold OR timer)
CREATE FUNCTION HybridWindow AS 'com.flink.ptf.HybridWindowFunction';

-- Early Fire Window PTF (emits on every event)
CREATE FUNCTION EarlyFireWindow AS 'com.flink.ptf.EarlyFireWindowFunction';
```

#### 3. Use the Hybrid Window PTF

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

This query:
- Partitions orders by `customer_id`
- Creates 5-second (5000ms) tumbling windows
- Emits early if more than 5000 orders are received in a window
- Aggregates orders and calculates total price per window
- Uses `ts` as the event time column

#### 4. Use the Early Fire Window PTF

Query using the early fire window. Execute the following SQL in the Flink SQL Client:

```sql
SELECT 
    customer_id,
    window_start,
    window_end,
    num_orders,
    total_price
FROM EarlyFireWindow(
    input => TABLE orders PARTITION BY customer_id,
    windowSizeMillis => CAST(5000 AS BIGINT),
    on_time => DESCRIPTOR(ts),
    uid => 'early-fire-win-v1'
);
```

This query:
- Partitions orders by `customer_id`
- Creates 5-second (5000ms) tumbling windows
- Emits immediately on every incoming event with current aggregation
- No count threshold - every event triggers an emission
- Uses `ts` as the event time column

## Understanding the PTFs

### HybridWindowFunction

The `HybridWindowFunction` class extends `ProcessTableFunction` and implements:

1. **Window State Management**: Tracks orders and aggregates data within each window partition
2. **Time-based Windowing**: Creates windows based on a duration in milliseconds (e.g., 5 seconds)
3. **Early Emission**: Emits window results when order count exceeds threshold
4. **Timer-based Expiration**: Uses Flink timers to emit windows at their end time
5. **Aggregation**: Calculates total price and order count per window

#### Parameters

- `windowSizeMillis`: The duration of each tumbling window in milliseconds. 
- `countThreshold`: Maximum number of orders before early emission (e.g., `5000`)
- `on_time`: Event time descriptor (e.g., `DESCRIPTOR(ts)`)
- `uid`: Unique identifier for the PTF instance (required for stateful transformations)

### EarlyFireWindowFunction

The `EarlyFireWindowFunction` class extends `ProcessTableFunction` and implements:

1. **Window State Management**: Tracks orders and aggregates data within each window partition
2. **Time-based Windowing**: Creates windows based on a duration in milliseconds (e.g., 5 seconds)
3. **Immediate Emission**: Emits window results on every incoming event
4. **No Timers**: Window transitions detected by comparing event timestamps to current window boundaries
5. **Aggregation**: Calculates total price and order count per window

#### Parameters

- `windowSizeMillis`: The duration of each tumbling window in milliseconds. 
- `on_time`: Event time descriptor (e.g., `DESCRIPTOR(ts)`)
- `uid`: Unique identifier for the PTF instance (required for stateful transformations)

**Important Note on `windowSizeMillis` Parameter**: Due to a known Flink issue ([FLINK-37618](https://issues.apache.org/jira/browse/FLINK-37618)), PTFs do not currently support `INTERVAL` arguments. You **must** use `CAST(5000 AS BIGINT)` instead of `INTERVAL '5' SECONDS` for the `windowSizeMillis` parameter. The value should be specified in milliseconds (e.g., `CAST(5000 AS BIGINT)` for 5 seconds).

### Output Schema (Both PTFs)

Both PTFs output:
- `window_start`: Start timestamp of the window (TIMESTAMP(3))
- `window_end`: End timestamp of the window (TIMESTAMP(3))
- `num_orders`: Number of orders in the window (INT)
- `total_price`: Total price of all orders in the window (DOUBLE)

## Stopping Docker Containers

To stop all containers:

```bash
docker-compose down
```

## Datagen Examples

The following examples use Flink's built-in Datagen connector to generate sample data, making them easy to run without external dependencies.

### 3. Anomaly Detector PTF (Enhanced)

The `AnomalyDetector` PTF demonstrates advanced state management with multiple state types for fraud detection:

**Advanced Features:**
- **MapView State**: Tracks merchant transaction counts (24-hour TTL)
- **ListView State**: Maintains transaction history for pattern analysis (7-day TTL)
- **Value State (POJO)**: Stores user profile with spending patterns (30-day TTL)
- **Multiple TTLs**: Different retention periods per state type

**State Management:**
- `userState`: POJO tracking lifetime stats, average transaction, last activity (30-day TTL)
- `merchantCounts`: MapView tracking transactions per merchant (24-hour TTL)
- `recentTransactions`: ListView of recent transaction records (7-day TTL)

**Usage:**

```sql
CREATE TABLE transactions (
    userId STRING,
    amount DOUBLE,
    merchantId STRING,
    ts BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'fields.userId.length' = '5',
    'fields.amount.min' = '1',
    'fields.amount.max' = '10000',
    'fields.merchantId.length' = '3',
    'fields.ts.kind' = 'sequence',
    'fields.ts.start' = '1',
    'fields.ts.end' = '10000000'
);

CREATE FUNCTION AnomalyDetector AS 'com.flink.ptf.AnomalyDetector';

SELECT userId, isAnomaly, anomalyScore, reason, transactionAmount, timestamp
FROM AnomalyDetector(
    input => TABLE transactions PARTITION BY userId,
    on_time => DESCRIPTOR(event_time),
    uid => 'anomaly-detector-v1'
);
```

**Output:** Emits `AnomalyResult` POJO with `(userId, isAnomaly, anomalyScore, reason, transactionAmount, timestamp)`.

### 4. Dynamic Pricing Engine PTF (Enhanced)

The `DynamicPricingEngine` PTF demonstrates multiple input streams and optional inputs for real-time pricing:

**Advanced Features:**
- **Multiple Input Streams**: Processes inventory updates AND competitor prices (2 tables)
- **Optional Inputs**: Competitor prices are optional (null checks in eval)
- **POJO Output**: Strongly-typed `PriceUpdate` output
- **Hardcoded Configuration**: Uses constants instead of scalar arguments (Flink limitation with multiple tables)

**Pricing Rules (Priority Order):**
1. Low Inventory (< 10 units) → 15% price increase
2. High Demand (> 100 signals) → 8% price increase
3. Competitor Matching → Undercut by 2%

**State Management:**
- `PricingState`: POJO tracking current price, inventory, competitor price, demand signals (7-day TTL)

**Usage:**

```sql
CREATE TABLE inventory (
    sku STRING,
    quantity INT,
    basePrice DOUBLE,
    ts BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '5',
    'fields.sku.length' = '2',
    'fields.quantity.min' = '1',
    'fields.quantity.max' = '50',
    'fields.basePrice.min' = '10',
    'fields.basePrice.max' = '100',
    'fields.ts.kind' = 'sequence',
    'fields.ts.start' = '1',
    'fields.ts.end' = '10000000'
);

CREATE TABLE competitor_prices (
    sku STRING,
    price DOUBLE,
    ts BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '3',
    'fields.sku.length' = '2',
    'fields.price.min' = '8',
    'fields.price.max' = '120',
    'fields.ts.kind' = 'sequence',
    'fields.ts.start' = '1',
    'fields.ts.end' = '100000'
);

CREATE FUNCTION DynamicPricingEngine AS 'com.flink.ptf.DynamicPricingEngine';

SELECT sku, newPrice, oldPrice, reason, `timestamp`
FROM DynamicPricingEngine(
    inventoryEvent => TABLE inventory PARTITION BY sku,
    competitorPriceEvent => TABLE competitor_prices PARTITION BY sku,
    uid => 'pricing-engine-v1'
);
```

**Output:** Emits `PriceUpdate` POJO with `(sku, newPrice, oldPrice, reason, timestamp)` when price changes exceed threshold.

### 5. Session Tracker PTF (Enhanced)

The `SessionTracker` PTF demonstrates timers and ListView for session management:

**Advanced Features:**
- **ListView State**: Stores all events within a session
- **Timers**: Uses `registerOnTime()` to detect session timeout
- **TimeContext**: Accesses event time for session expiration
- **Scalar Arguments**: Accepts `sessionTimeoutMillis` parameter

**Session Logic:**
1. Each event extends the session timeout
2. On timeout (via timer), emits session summary with all events
3. ListView stores event history for aggregation

**State Management:**
- `SessionState`: POJO tracking session start, event count, total value (2-hour TTL)
- `events`: ListView of session events

**Usage:**

```sql
CREATE TABLE user_events (
    userId STRING,
    eventType STRING,
    eventValue DOUBLE,
    ts BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '5',
    'fields.userId.length' = '3',
    'fields.eventType.length' = '5',
    'fields.eventValue.min' = '1',
    'fields.eventValue.max' = '100',
    'fields.ts.kind' = 'sequence',
    'fields.ts.start' = '1',
    'fields.ts.end' = '10000000'
);

CREATE FUNCTION SessionTracker AS 'com.flink.ptf.SessionTracker';

SELECT userId, sessionStart, sessionEnd, eventCount, totalValue, eventTypes
FROM SessionTracker(
    input => TABLE user_events PARTITION BY userId,
    sessionTimeoutMillis => CAST(30000 AS BIGINT),
    on_time => DESCRIPTOR(event_time),
    uid => 'session-tracker-v1'
);
```

**Output:** Emits `SessionSummary` POJO with `(userId, sessionStart, sessionEnd, eventCount, totalValue, eventTypes)`.

### 6. First Match Join PTF (Enhanced)

The `FirstMatchJoin` PTF demonstrates a specialized join pattern with multiple inputs:

**Advanced Features:**
- **Multiple Input Streams**: Processes orders AND customers (2 tables)
- **First-Match Semantics**: Emits only once when both sides arrive (no updates)
- **POJO Output**: Strongly-typed `EnrichedOrder` output
- **TimeContext**: Uses event time for join timestamp

**Join Semantics:**
1. Waits for first arrival from both order and customer streams
2. Emits exactly one enriched result
3. Sets flag to prevent future emissions for this key
4. Subsequent updates are ignored

**State Management:**
- `FirstMatchState`: POJO tracking order data, customer data, join metadata (1-hour TTL)

**Usage:**

```sql
CREATE TABLE orders (
    orderId STRING,
    customerId INT,
    totalAmount DOUBLE,
    ts BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '5',
    'fields.orderId.length' = '8',
    'fields.customerId.min' = '1',
    'fields.customerId.max' = '100',
    'fields.totalAmount.min' = '10',
    'fields.totalAmount.max' = '500',
    'fields.ts.kind' = 'sequence',
    'fields.ts.start' = '1',
    'fields.ts.end' = '10000000'
);

CREATE TABLE customers (
    customerId INT,
    name STRING,
    email STRING,
    tier STRING,
    ts BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '3',
    'fields.customerId.min' = '1',
    'fields.customerId.max' = '100',
    'fields.name.length' = '10',
    'fields.email.length' = '15',
    'fields.tier.length' = '4',
    'fields.ts.kind' = 'sequence',
    'fields.ts.start' = '1',
    'fields.ts.end' = '10000000'
);

CREATE FUNCTION FirstMatchJoin AS 'com.flink.ptf.FirstMatchJoin';

SELECT orderId, customerId, totalAmount, customerName, customerEmail, customerTier, firstArrival, joinTimestamp
FROM FirstMatchJoin(
    orderStream => TABLE orders PARTITION BY customerId,
    customerStream => TABLE customers PARTITION BY customerId,
    on_time => DESCRIPTOR(event_time),
    uid => 'first-match-join-v1'
);
```

**Output:** Emits `EnrichedOrder` POJO with order + customer columns and join metadata.
