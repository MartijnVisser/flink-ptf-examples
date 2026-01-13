package com.flink.ptf;

import static org.apache.flink.table.annotation.ArgumentTrait.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * A ProcessTableFunction that denormalizes Debezium CDC transactions into single events.
 *
 * <p>This PTF demonstrates how to aggregate multiple CDC events from different tables that belong
 * to the same database transaction into a single denormalized event.
 *
 * <p><b>Key Feature:</b> Uses Debezium's transaction metadata ({@code transaction.id} and {@code
 * transaction.total_order}) to detect when all events from a transaction have arrived, then emits a
 * complete denormalized record.
 *
 * <h3>Debezium Transaction Pattern</h3>
 *
 * <p>Debezium CDC events include transaction metadata:
 *
 * <ul>
 *   <li><b>transaction.id</b>: Unique identifier for the database transaction (e.g.,
 *       "12345:98765432")
 *   <li><b>transaction.total_order</b>: Total number of events in the transaction (monotonically
 *       increasing)
 *   <li><b>transaction.data_collection_order</b>: Sequence within each table
 *   <li><b>source.txId</b>: Database transaction ID
 *   <li><b>source.lsn</b>: Log Sequence Number (PostgreSQL)
 * </ul>
 *
 * <h3>Use Case: E-Commerce Order Transaction</h3>
 *
 * <p>A single database transaction creates an order and inserts line items:
 *
 * <pre>
 * Transaction ID: "12345:98765432"
 * Event 1: INSERT orders (order_id=1001, customer_id=42, status=pending, total=299.99)
 * Event 2: INSERT order_items (product_id=777, quantity=2, unit_price=99.99)
 * Event 3: INSERT order_items (product_id=888, quantity=1, unit_price=100.01)
 * Event 4: UPDATE orders (status=confirmed)
 *
 * These 4 events arrive on 2 different Kafka topics but share the same transaction.id.
 * </pre>
 *
 * <h3>Output Schema</h3>
 *
 * <p>Denormalized event containing:
 *
 * <ul>
 *   <li><b>transaction_id:</b> Debezium transaction identifier
 *   <li><b>order_id:</b> Order ID from orders table
 *   <li><b>customer_id:</b> Customer ID
 *   <li><b>status:</b> Final order status (after all updates)
 *   <li><b>total_amount:</b> Total order amount
 *   <li><b>line_items:</b> Array of line items with product_id, quantity, unit_price
 * </ul>
 *
 * <h3>Transaction Completion Detection</h3>
 *
 * <p>The PTF uses Debezium's transaction boundary events for authoritative completion detection:
 *
 * <pre>
 * Event 0: BEGIN transaction (status=BEGIN, optional)
 * Event 1: INSERT order (change event 1/4)
 * Event 2: INSERT line item (change event 2/4)
 * Event 3: INSERT line item (change event 3/4)
 * Event 4: UPDATE order (change event 4/4)
 * Event 5: END transaction (status=END, event_count=4) ← Authoritative completion signal
 *
 * Completion criteria:
 * 1. END event received → sets expectedEventCount = 4
 * 2. All 4 change events received → receivedEventCount = 4
 * 3. When both conditions met → EMIT denormalized event!
 * </pre>
 *
 * <h3>Transaction Boundary Topic</h3>
 *
 * <p>Debezium emits transaction metadata to a separate topic: {@code <server>.transaction}
 *
 * <p>BEGIN event (emitted at transaction start):
 *
 * <pre>
 * {
 *   "status": "BEGIN",
 *   "id": "12345:98765432",
 *   "ts_ms": 1718294400123,
 *   "event_count": null
 * }
 * </pre>
 *
 * <p>END event (emitted after all change events):
 *
 * <pre>
 * {
 *   "status": "END",
 *   "id": "12345:98765432",
 *   "ts_ms": 1718294400230,
 *   "event_count": 4,
 *   "data_collections": [
 *     {"data_collection": "public.orders", "event_count": 2},
 *     {"data_collection": "public.order_items", "event_count": 2}
 *   ]
 * }
 * </pre>
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * -- Create Kafka tables for each Debezium topic
 * CREATE TABLE orders_cdc (
 *     `before` ROW<id BIGINT, customer_id BIGINT, status STRING, total_amount DOUBLE>,
 *     `after` ROW<id BIGINT, customer_id BIGINT, status STRING, total_amount DOUBLE>,
 *     `source` ROW<txId BIGINT, lsn BIGINT, `table` STRING>,
 *     op STRING,
 *     ts_ms BIGINT,
 *     `transaction` ROW<id STRING, total_order BIGINT, data_collection_order BIGINT>
 * ) WITH (
 *     'connector' = 'kafka',
 *     'topic' = 'inventory.public.orders',
 *     'properties.bootstrap.servers' = 'localhost:9092',
 *     'format' = 'debezium-json'
 * );
 *
 * CREATE TABLE order_items_cdc (
 *     `before` ROW<id BIGINT, order_id BIGINT, product_id BIGINT, quantity INT, unit_price DOUBLE>,
 *     `after` ROW<id BIGINT, order_id BIGINT, product_id BIGINT, quantity INT, unit_price DOUBLE>,
 *     `source` ROW<txId BIGINT, lsn BIGINT, `table` STRING>,
 *     op STRING,
 *     ts_ms BIGINT,
 *     `transaction` ROW<id STRING, total_order BIGINT, data_collection_order BIGINT>
 * ) WITH (
 *     'connector' = 'kafka',
 *     'topic' = 'inventory.public.order_items',
 *     'properties.bootstrap.servers' = 'localhost:9092',
 *     'format' = 'debezium-json'
 * );
 *
 * -- Create Kafka table for transaction boundary events (BEGIN/END)
 * CREATE TABLE transaction_events (
 *     status STRING,
 *     id STRING,
 *     ts_ms BIGINT,
 *     event_count BIGINT,
 *     data_collections ARRAY<ROW<data_collection STRING, event_count BIGINT>>
 * ) WITH (
 *     'connector' = 'kafka',
 *     'topic' = 'inventory.transaction',
 *     'properties.bootstrap.servers' = 'localhost:9092',
 *     'format' = 'debezium-json'
 * );
 *
 * -- Register PTF
 * CREATE FUNCTION DebeziumTransactionDenormalizer AS 'com.flink.ptf.DebeziumTransactionDenormalizer';
 *
 * -- Denormalize transactions (partition all inputs by transaction.id)
 * SELECT *
 * FROM DebeziumTransactionDenormalizer(
 *     ordersEvent => TABLE orders_cdc PARTITION BY `transaction`.id,
 *     orderItemsEvent => TABLE order_items_cdc PARTITION BY `transaction`.id,
 *     transactionEvent => TABLE transaction_events PARTITION BY id,
 *     uid => 'debezium-denormalizer-v1'
 * );
 * </pre>
 *
 * <h3>Example Output</h3>
 *
 * <pre>
 * transaction_id    | order_id | customer_id | status    | total_amount | line_items
 * ------------------|----------|-------------|-----------|--------------|----------------------------------
 * 12345:98765432    | 1001     | 42          | confirmed | 299.99       | [(777, 2, 99.99), (888, 1, 100.01)]
 * </pre>
 *
 * <h3>State Management</h3>
 *
 * <p>Maintains per-transaction state with 1-hour TTL:
 *
 * <ul>
 *   <li><b>TransactionState:</b> Buffers order header and line items until transaction completes
 *   <li><b>receivedCount:</b> Tracks number of events received
 *   <li><b>totalOrder:</b> Expected total events from transaction metadata
 *   <li><b>lineItems:</b> List of collected line items
 * </ul>
 *
 * <h3>Key Features Demonstrated</h3>
 *
 * <ul>
 *   <li>Multiple input streams (orders + order_items CDC topics)
 *   <li>Transaction boundary detection using Debezium metadata
 *   <li>Stateful aggregation with completion detection
 *   <li>Nested collection state (List of line items)
 *   <li>Automatic state cleanup on transaction completion
 * </ul>
 *
 * <h3>Use Cases</h3>
 *
 * <ul>
 *   <li>Denormalizing CDC events for analytics/OLAP systems
 *   <li>Building transaction-consistent snapshots
 *   <li>Aggregating multi-table database transactions
 *   <li>Creating materialized views from CDC streams
 *   <li>Event-driven architectures requiring consistent transaction boundaries
 * </ul>
 *
 * <h3>Production Considerations</h3>
 *
 * <ul>
 *   <li><b>Out-of-order events:</b> Events may arrive out of sequence; total_order handles this
 *   <li><b>Incomplete transactions:</b> State TTL handles abandoned transactions
 *   <li><b>Large transactions:</b> Consider max line item limits to prevent OOM
 *   <li><b>Duplicate events:</b> Use idempotency keys if needed
 * </ul>
 */
public class DebeziumTransactionDenormalizer
    extends ProcessTableFunction<DebeziumTransactionDenormalizer.DenormalizedTransaction> {

  /** POJO output type for denormalized transactions. */
  public static class DenormalizedTransaction {
    public String transaction_id;
    public Long order_id;
    public Long customer_id;
    public String status;
    public Double total_amount;
    public LineItem[] line_items;
  }

  /** POJO for line items in the denormalized output. */
  public static class LineItem {
    public Long product_id;
    public Integer quantity;
    public Double unit_price;

    public LineItem() {}

    public LineItem(Long product_id, Integer quantity, Double unit_price) {
      this.product_id = product_id;
      this.quantity = quantity;
      this.unit_price = unit_price;
    }
  }

  /** State class tracking a single database transaction as it accumulates events. */
  public static class TransactionState {
    // Order header fields (from orders table)
    public Long orderId = null;
    public Long customerId = null;
    public String status = null;
    public Double totalAmount = null;

    // Transaction tracking (from END event on transaction topic)
    public Integer expectedEventCount = null; // From END event's event_count field
    public int receivedEventCount = 0; // Actual change events received so far
    public boolean endEventReceived = false; // Has END event arrived?

    // Line items collected (from order_items table)
    public List<LineItem> lineItems = new ArrayList<>();
  }

  /**
   * Processes CDC events from multiple tables plus transaction boundary events.
   *
   * <p>This method receives:
   *
   * <ul>
   *   <li>Change events from orders and order_items tables
   *   <li>Transaction boundary events (BEGIN/END) from the transaction topic
   * </ul>
   *
   * <p>The END event provides the authoritative completion signal with event_count.
   *
   * @param ctx Context for state management
   * @param state Per-transaction state tracking order header, line items, and completion
   * @param ordersEvent CDC event from orders table (INSERT or UPDATE)
   * @param orderItemsEvent CDC event from order_items table (INSERT)
   * @param transactionEvent BEGIN or END event from transaction boundary topic
   */
  public void eval(
      Context ctx,
      @StateHint(ttl = "1 hour") TransactionState state,
      @ArgumentHint(
              value = SET_SEMANTIC_TABLE,
              type =
                  @DataTypeHint(
                      "ROW<`before` ROW<id BIGINT, customer_id BIGINT, status STRING, total_amount DOUBLE>, `after` ROW<id BIGINT, customer_id BIGINT, status STRING, total_amount DOUBLE>, `source` ROW<version STRING, connector STRING, name STRING, ts_ms BIGINT, db STRING, `schema` STRING, `table` STRING, txId BIGINT, lsn BIGINT, xmin BIGINT>, op STRING, ts_ms BIGINT, `transaction` ROW<id STRING, total_order BIGINT, data_collection_order BIGINT>, transaction_id STRING>"))
          Row ordersEvent,
      @ArgumentHint(
              value = SET_SEMANTIC_TABLE,
              type =
                  @DataTypeHint(
                      "ROW<`before` ROW<id BIGINT, order_id BIGINT, product_id BIGINT, quantity INT, unit_price DOUBLE>, `after` ROW<id BIGINT, order_id BIGINT, product_id BIGINT, quantity INT, unit_price DOUBLE>, `source` ROW<version STRING, connector STRING, name STRING, ts_ms BIGINT, db STRING, `schema` STRING, `table` STRING, txId BIGINT, lsn BIGINT, xmin BIGINT>, op STRING, ts_ms BIGINT, `transaction` ROW<id STRING, total_order BIGINT, data_collection_order BIGINT>, transaction_id STRING>"))
          Row orderItemsEvent,
      @ArgumentHint(
              value = SET_SEMANTIC_TABLE,
              type =
                  @DataTypeHint(
                      "ROW<status STRING, id STRING, ts_ms BIGINT, event_count BIGINT, data_collections ARRAY<ROW<data_collection STRING, event_count BIGINT>>>"))
          Row transactionEvent)
      throws Exception {

    String transactionId = null;

    // === Handle transaction boundary events (BEGIN/END) ===
    if (transactionEvent != null) {
      String status = transactionEvent.getFieldAs("status");
      transactionId = transactionEvent.getFieldAs("id");

      if ("END".equals(status)) {
        // END event tells us the total number of change events to expect
        state.expectedEventCount = ((Number) transactionEvent.getFieldAs("event_count")).intValue();
        state.endEventReceived = true;
      }
      // BEGIN event can be ignored - we don't need it for completion detection
    }

    // === Handle orders table event ===
    if (ordersEvent != null) {
      // Extract transaction ID
      Row transaction = ordersEvent.getFieldAs("transaction");
      transactionId = transaction.getFieldAs("id");

      // Extract order data from 'after' field (INSERT or UPDATE)
      Row after = ordersEvent.getFieldAs("after");
      if (after != null) {
        state.orderId = ((Number) after.getFieldAs("id")).longValue();
        state.customerId = ((Number) after.getFieldAs("customer_id")).longValue();
        state.status = after.getFieldAs("status");
        state.totalAmount = ((Number) after.getFieldAs("total_amount")).doubleValue();
      }

      state.receivedEventCount++;
    }

    // === Handle order_items table event ===
    if (orderItemsEvent != null) {
      // Extract transaction ID
      Row transaction = orderItemsEvent.getFieldAs("transaction");
      transactionId = transaction.getFieldAs("id");

      // Extract line item data from 'after' field (INSERT)
      Row after = orderItemsEvent.getFieldAs("after");
      if (after != null) {
        LineItem lineItem =
            new LineItem(
                ((Number) after.getFieldAs("product_id")).longValue(),
                ((Number) after.getFieldAs("quantity")).intValue(),
                ((Number) after.getFieldAs("unit_price")).doubleValue());

        state.lineItems.add(lineItem);
      }

      state.receivedEventCount++;
    }

    // === Check for transaction completion ===
    // Transaction is complete when:
    // 1. END event has been received (tells us expected count)
    // 2. We've received all expected change events
    if (state.endEventReceived
        && state.expectedEventCount != null
        && state.receivedEventCount == state.expectedEventCount) {

      // All events from transaction have arrived - emit denormalized event

      // Convert List to array for POJO
      LineItem[] lineItemsArray = state.lineItems.toArray(new LineItem[0]);

      // Emit denormalized transaction as POJO
      DenormalizedTransaction result = new DenormalizedTransaction();
      result.transaction_id = transactionId;
      result.order_id = state.orderId;
      result.customer_id = state.customerId;
      result.status = state.status;
      result.total_amount = state.totalAmount;
      result.line_items = lineItemsArray;

      collect(result);

      // Clear state for next transaction (important to prevent state growth!)
      ctx.clearAll();
    }
  }
}
