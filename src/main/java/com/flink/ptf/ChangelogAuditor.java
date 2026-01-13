package com.flink.ptf;

import static org.apache.flink.table.annotation.ArgumentTrait.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * A ProcessTableFunction that demonstrates consuming changelog streams (CDC) and converting them to
 * an append-only audit trail.
 *
 * <p>This PTF showcases advanced changelog handling features:
 *
 * <ul>
 *   <li><b>SUPPORT_UPDATES:</b> Consumes tables with INSERT/UPDATE/DELETE operations
 *   <li><b>State Management:</b> Tracks previous values to show before/after changes
 *   <li><b>Changelog to Append-Only:</b> Converts updating table to immutable audit log
 *   <li><b>CDC Use Case:</b> Real-world pattern for audit trails and change tracking
 * </ul>
 *
 * <h3>Input Requirements</h3>
 *
 * <p>This PTF consumes a changelog stream (updating table) that contains:
 *
 * <ul>
 *   <li>RowKind.INSERT: New currency rate introduced
 *   <li>RowKind.UPDATE_AFTER: Currency rate changed (in upsert mode)
 *   <li>RowKind.UPDATE_BEFORE + UPDATE_AFTER: Currency rate changed (in retract mode)
 *   <li>RowKind.DELETE: Currency rate removed
 * </ul>
 *
 * <h3>Output Schema</h3>
 *
 * <p>Produces an append-only audit trail with the following columns:
 *
 * <ul>
 *   <li><b>change_type:</b> Type of change (INSERT, UPDATE, DELETE)
 *   <li><b>currency:</b> The currency being tracked
 *   <li><b>old_rate:</b> Previous rate value (NULL for INSERT)
 *   <li><b>new_rate:</b> New rate value (NULL for DELETE)
 *   <li><b>change_time:</b> When the change occurred
 * </ul>
 *
 * <h3>Typical Usage with Versioned Views</h3>
 *
 * <pre>
 * -- Step 1: Create append-only source (e.g., from Datagen or Kafka JSON)
 * CREATE TABLE currency_rates (
 *     currency      STRING,
 *     rate          DECIMAL(10, 4),
 *     update_time   TIMESTAMP(3),
 *     WATERMARK FOR update_time AS update_time - INTERVAL '1' SECOND
 * ) WITH ('connector' = 'datagen', ...);
 *
 * -- Step 2: Create versioned view to generate changelog
 * CREATE VIEW versioned_rates AS
 * SELECT currency, rate, update_time
 * FROM (
 *     SELECT *,
 *         ROW_NUMBER() OVER (PARTITION BY currency ORDER BY update_time DESC) AS rownum
 *     FROM currency_rates
 * )
 * WHERE rownum = 1;
 *
 * -- Step 3: Register and use the ChangelogAuditor PTF
 * CREATE FUNCTION ChangelogAuditor AS 'com.flink.ptf.ChangelogAuditor';
 *
 * SELECT * FROM ChangelogAuditor(
 *     input => TABLE versioned_rates PARTITION BY currency,
 *     uid => 'changelog-auditor-v1'
 * );
 * </pre>
 *
 * <h3>Example Output</h3>
 *
 * <pre>
 * change_type | currency | old_rate | new_rate | change_time
 * ------------|----------|----------|----------|------------------------
 * INSERT      | EUR      | NULL     | 1.0850   | 2025-03-27 09:00:00.000
 * INSERT      | USD      | NULL     | 1.0000   | 2025-03-27 09:00:00.000
 * UPDATE      | EUR      | 1.0850   | 1.0920   | 2025-03-27 09:15:00.000
 * INSERT      | GBP      | NULL     | 0.8650   | 2025-03-27 09:30:00.000
 * UPDATE      | EUR      | 1.0920   | 1.0875   | 2025-03-27 09:45:00.000
 * DELETE      | GBP      | 0.8650   | NULL     | 2025-03-27 10:00:00.000
 * </pre>
 *
 * <h3>State Management</h3>
 *
 * <p>Maintains per-currency state with 7-day TTL:
 *
 * <ul>
 *   <li><b>previousRate:</b> Last known rate for calculating before/after values
 * </ul>
 *
 * <p>State is partitioned by currency (as specified in PARTITION BY clause).
 *
 * <h3>Use Cases</h3>
 *
 * <ul>
 *   <li>Audit logging for compliance and debugging
 *   <li>Change data capture (CDC) from databases
 *   <li>Tracking dimension table changes over time
 *   <li>Building slowly changing dimension (SCD) Type 2 tables
 *   <li>Monitoring data quality and detecting anomalies in updates
 * </ul>
 */
@DataTypeHint(
    "ROW<change_type STRING, currency STRING, old_rate DECIMAL(10, 4), new_rate DECIMAL(10, 4), change_time TIMESTAMP(3)>")
public class ChangelogAuditor extends ProcessTableFunction<Row> {

  /** State class tracking the previous rate value for each currency. */
  public static class RateState {
    @DataTypeHint("DECIMAL(10, 4)")
    public BigDecimal previousRate = null;
  }

  /**
   * Processes changelog events and emits audit trail records.
   *
   * <p>This method receives rows with different RowKind values:
   *
   * <ul>
   *   <li>INSERT: First time seeing this currency
   *   <li>UPDATE_AFTER: Currency rate changed (upsert mode)
   *   <li>UPDATE_BEFORE: Old value being retracted (retract mode, optional)
   *   <li>DELETE: Currency removed
   * </ul>
   *
   * @param state Per-currency state tracking previous rate
   * @param input Changelog row containing currency, rate, update_time
   */
  public void eval(
      @StateHint(ttl = "7 days") RateState state,
      @ArgumentHint({SET_SEMANTIC_TABLE, SUPPORT_UPDATES}) Row input) {

    // Extract RowKind to determine the type of change
    RowKind kind = input.getKind();

    // Extract fields from the input row
    String currency = input.getFieldAs("currency");
    BigDecimal rate = input.getFieldAs("rate");
    LocalDateTime updateTime = input.getFieldAs("update_time");

    // Process based on changelog operation type
    switch (kind) {
      case INSERT:
        // New currency introduced - no previous rate
        collect(Row.of("INSERT", currency, null, rate, updateTime));
        state.previousRate = rate;
        break;

      case UPDATE_AFTER:
        // Currency rate changed - show before and after
        collect(Row.of("UPDATE", currency, state.previousRate, rate, updateTime));
        state.previousRate = rate;
        break;

      case UPDATE_BEFORE:
        // In retract mode, we receive both UPDATE_BEFORE and UPDATE_AFTER
        // We can ignore UPDATE_BEFORE since UPDATE_AFTER has all the info we need
        // and we already track previousRate in state
        break;

      case DELETE:
        // Currency removed - show last known rate
        collect(Row.of("DELETE", currency, state.previousRate, null, updateTime));
        state.previousRate = null;
        break;
    }
  }
}
