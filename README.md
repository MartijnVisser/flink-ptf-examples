# Apache Flink ProcessTableFunction Examples

This repository contains example implementations of Apache Flink ProcessTableFunctions (PTFs) that connect to Confluent Cloud Kafka clusters using Avro Confluent format.

## Overview

This project demonstrates how to:
- Create custom ProcessTableFunctions in Apache Flink 2.2
- Connect to Confluent Cloud Kafka using the Kafka connector 4.0.1-2.0
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
- Java 11+

**For Kafka examples (HybridWindow, EarlyFireWindow) only:**
- Confluent Cloud account with:
  - Kafka cluster
  - Schema Registry
  - API keys and secrets

**Note:** Datagen examples (3-10) work standalone without any external dependencies.

## Setup

### 1. Build the Project

Build the Maven project to create the PTF JAR:

```bash
mvn clean package
```

This will:
- Compile the Java code
- Package the PTF classes into a JAR file
- Place the JAR in `target/flink-ptf-examples-1.0.2.jar`

### 2. Build Docker Image

Build the Docker image that includes Flink, Kafka connector, Avro format, and the PTF:

```bash
docker-compose build --no-cache
```

This Dockerfile automatically:
- Uses Flink 2.2.0 base image
- Downloads and installs Flink Kafka connector 4.0.1-2.0
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

### 7. Changelog Auditor PTF (CDC/Change Data Capture)

The `ChangelogAuditor` PTF demonstrates consuming changelog streams with SUPPORT_UPDATES:

**Advanced Features:**
- **SUPPORT_UPDATES**: Consumes changelog streams (INSERT, UPDATE, DELETE)
- **RowKind Handling**: Inspects `input.getKind()` to determine change type
- **Audit Trail**: Converts changelog to append-only audit log

**Changelog Types:**
- `INSERT (+I)`: New record
- `UPDATE_AFTER (+U)`: Updated record (new value)
- `UPDATE_BEFORE (-U)`: Retracted value (old value, optional)
- `DELETE (-D)`: Deleted record

**State Management:**
- `RateState`: POJO tracking last known rate per currency (7-day TTL)

**Usage:**

```sql
-- Create source table
CREATE TABLE currency_rates (
    currency_idx INT,
    currency AS CASE currency_idx
        WHEN 0 THEN 'EUR' WHEN 1 THEN 'USD' WHEN 2 THEN 'GBP'
        WHEN 3 THEN 'JPY' WHEN 4 THEN 'CHF' ELSE 'UNK'
    END,
    rate DECIMAL(10, 4),
    update_time TIMESTAMP(3),
    WATERMARK FOR update_time AS update_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '2',
    'fields.currency_idx.min' = '0',
    'fields.currency_idx.max' = '4',
    'fields.rate.min' = '0.5',
    'fields.rate.max' = '2.0'
);

-- Create versioned view that produces changelog
CREATE VIEW versioned_rates AS
SELECT currency, rate, update_time
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY update_time DESC) AS rownum
    FROM currency_rates
)
WHERE rownum = 1;

CREATE FUNCTION ChangelogAuditor AS 'com.flink.ptf.ChangelogAuditor';

SELECT change_type, currency, old_rate, new_rate, change_time
FROM ChangelogAuditor(
    input => TABLE versioned_rates PARTITION BY currency,
    uid => 'changelog-auditor-v1'
);
```

**Output:** Emits Row with `(change_type, currency, old_rate, new_rate, change_time)`.

### 8. Data Quality Scorer PTF (PASS_COLUMNS_THROUGH)

The `DataQualityScorer` PTF demonstrates automatic column forwarding with PASS_COLUMNS_THROUGH:

**Advanced Features:**
- **PASS_COLUMNS_THROUGH**: Preserves all input columns in output automatically
- **ROW_SEMANTIC_TABLE**: Stateless row-by-row processing
- **No State**: Purely computational (no POJO state, no TTL)
- **Validation**: Checks data quality and emits scores

**How PASS_COLUMNS_THROUGH Works:**
1. PTF defines ONLY new output columns (quality_score, quality_issues, is_valid)
2. All input columns are automatically forwarded
3. No manual column handling needed

**Validation Rules:**
- Amount range check (> 0 and < 1,000,000)
- Country validation (ISO codes)
- UserId format check

**Usage:**

```sql
CREATE TABLE transactions (
    transactionId STRING,
    userId STRING,
    amount DOUBLE,
    country STRING,
    ts BIGINT
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'fields.transactionId.length' = '10',
    'fields.userId.length' = '5',
    'fields.amount.min' = '-100',
    'fields.amount.max' = '10000',
    'fields.country.length' = '2',
    'fields.ts.kind' = 'sequence',
    'fields.ts.start' = '1',
    'fields.ts.end' = '10000000'
);

CREATE FUNCTION DataQualityScorer AS 'com.flink.ptf.DataQualityScorer';

-- Output includes ALL input columns PLUS quality_score, quality_issues, is_valid
SELECT *
FROM DataQualityScorer(
    input => TABLE transactions,
    uid => 'data-quality-v1'
);
```

**Output:** Input columns + `(quality_score, quality_issues, is_valid)`.

**Note:** PASS_COLUMNS_THROUGH only works with single table argument, append-only PTFs, no timers.

### 9. Row Formatter PTF (Polymorphic Table Arguments)

The `RowFormatter` PTF demonstrates polymorphic table arguments by accepting tables with ANY schema:

**Advanced Features:**
- **Polymorphic Arguments**: Accepts tables with any schema (no `@DataTypeHint` on Row parameter)
- **Context Schema Inspection**: Uses `Context.tableSemanticsFor()` to discover schema at runtime
- **MapView State**: Tracks unique row patterns per partition
- **Dynamic Formatting**: Formats rows based on runtime-discovered partition keys and schema

**How Polymorphic Arguments Work:**
```java
// NON-polymorphic: Explicit schema required (uses type= inside @ArgumentHint)
public void eval(
    @ArgumentHint(value = SET_SEMANTIC_TABLE, type = @DataTypeHint("ROW<id INT, name STRING>"))
    Row input) {
  // Only works with tables matching this exact schema
}

// POLYMORPHIC: No type hint, accepts ANY schema
public void eval(Context ctx, @ArgumentHint(SET_SEMANTIC_TABLE) Row input) {
  var semantics = ctx.tableSemanticsFor("input");
  DataType actualSchema = semantics.dataType(); // Discover schema at runtime
  int[] keys = semantics.partitionByColumns();  // Discover partition keys
}
```

**State Management:**
- `seenPatterns`: MapView tracking unique row signatures (24-hour TTL)

**Usage:**

```sql
-- Works with ANY table!
CREATE TABLE users (id INT, name STRING, age INT) WITH ('connector' = 'datagen', ...);
CREATE TABLE orders (order_id STRING, customer_id INT, price DOUBLE) WITH ('connector' = 'datagen', ...);

CREATE FUNCTION RowFormatter AS 'com.flink.ptf.RowFormatter';

-- Same PTF works with different schemas
SELECT * FROM RowFormatter(input => TABLE users PARTITION BY id, uid => 'rf-users');
SELECT * FROM RowFormatter(input => TABLE orders PARTITION BY customer_id, uid => 'rf-orders');
```

**Output:** `(formatted_row, schema_info, distinct_patterns)` - works with any input schema.

**Note:** This is the only example demonstrating polymorphic table arguments and runtime schema inspection via Context APIs.

### 10. Debezium Transaction Denormalizer PTF (Transaction Aggregation)

The `DebeziumTransactionDenormalizer` PTF aggregates multiple CDC events from different tables that belong to the same database transaction into a single denormalized event:

**Advanced Features:**
- **Multiple Input Streams**: Processes orders CDC + order_items CDC + transaction boundary events
- **Transaction Boundary Detection**: Uses Debezium's transaction metadata (BEGIN/END events)
- **Completion Detection**: Waits for all events before emitting
- **Nested Collections**: Stores line items as List in POJO state
- **POJO Output**: Strongly-typed `DenormalizedTransaction` with `LineItem[]` array

**How It Works:**
1. Receives CDC events from multiple tables (orders, order_items)
2. Buffers events in state, partitioned by transaction ID
3. Listens for END event from Debezium transaction topic
4. When all expected events arrive, emits denormalized result
5. Clears state after emission

**State Management:**
- `TransactionState`: POJO with order header, line items list, event counts (1-hour TTL)

**Test Data:**

Test data is provided in `test-data/debezium/` and baked into the Docker image. Use filesystem connector for testing without Kafka:

```sql
-- Create filesystem tables for testing
CREATE TABLE orders_cdc (
    `before` ROW<id BIGINT, customer_id BIGINT, status STRING, total_amount DOUBLE>,
    `after` ROW<id BIGINT, customer_id BIGINT, status STRING, total_amount DOUBLE>,
    `source` ROW<version STRING, connector STRING, name STRING, ts_ms BIGINT, db STRING, `schema` STRING, `table` STRING, txId BIGINT, lsn BIGINT, xmin BIGINT>,
    op STRING,
    ts_ms BIGINT,
    `transaction` ROW<id STRING, total_order BIGINT, data_collection_order BIGINT>,
    transaction_id AS `transaction`.id
) WITH (
    'connector' = 'filesystem',
    'path' = '/opt/flink/test-data/debezium/orders.json',
    'format' = 'json'
);

CREATE TABLE order_items_cdc (
    `before` ROW<id BIGINT, order_id BIGINT, product_id BIGINT, quantity INT, unit_price DOUBLE>,
    `after` ROW<id BIGINT, order_id BIGINT, product_id BIGINT, quantity INT, unit_price DOUBLE>,
    `source` ROW<version STRING, connector STRING, name STRING, ts_ms BIGINT, db STRING, `schema` STRING, `table` STRING, txId BIGINT, lsn BIGINT, xmin BIGINT>,
    op STRING,
    ts_ms BIGINT,
    `transaction` ROW<id STRING, total_order BIGINT, data_collection_order BIGINT>,
    transaction_id AS `transaction`.id
) WITH (
    'connector' = 'filesystem',
    'path' = '/opt/flink/test-data/debezium/order_items.json',
    'format' = 'json'
);

CREATE TABLE transaction_events (
    status STRING,
    id STRING,
    ts_ms BIGINT,
    event_count BIGINT,
    data_collections ARRAY<ROW<data_collection STRING, event_count BIGINT>>
) WITH (
    'connector' = 'filesystem',
    'path' = '/opt/flink/test-data/debezium/transaction.json',
    'format' = 'json'
);

CREATE FUNCTION DebeziumTransactionDenormalizer AS 'com.flink.ptf.DebeziumTransactionDenormalizer';

-- Execute (excluding line_items for display - SQL Client can't show nested arrays)
SELECT transaction_id, order_id, customer_id, status, total_amount
FROM DebeziumTransactionDenormalizer(
  ordersEvent => TABLE orders_cdc PARTITION BY transaction_id,
  orderItemsEvent => TABLE order_items_cdc PARTITION BY transaction_id,
  transactionEvent => TABLE transaction_events PARTITION BY id,
  uid => 'debezium-filesystem-test'
);

-- Verify line_items array is populated
SELECT transaction_id, order_id, customer_id, status, total_amount,
       CARDINALITY(line_items) as num_line_items
FROM DebeziumTransactionDenormalizer(
  ordersEvent => TABLE orders_cdc PARTITION BY transaction_id,
  orderItemsEvent => TABLE order_items_cdc PARTITION BY transaction_id,
  transactionEvent => TABLE transaction_events PARTITION BY id,
  uid => 'debezium-filesystem-test'
);
```

**Output:** Emits `DenormalizedTransaction` POJO with `(transaction_id, order_id, customer_id, status, total_amount, line_items)` per transaction.

**Note:** The `line_items` field contains a `LineItem[]` array with product details. The SQL Client cannot display nested arrays in table view, but the data is correctly populated. Use `CARDINALITY(line_items)` to verify or sink to Kafka/filesystem for full output.
