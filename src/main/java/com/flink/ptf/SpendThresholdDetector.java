package com.flink.ptf;

import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

import org.apache.flink.table.annotation.*;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * A ProcessTableFunction that emits exactly once when a customer's cumulative spending crosses a
 * threshold.
 *
 * <h3>Why This Can't Be Done in SQL</h3>
 *
 * <p>This combines two things SQL can't do together:
 *
 * <ol>
 *   <li><b>Running aggregation</b>: Track cumulative spend across all orders
 *   <li><b>First-occurrence emission</b>: Emit exactly once when threshold is crossed, then never
 *       again
 * </ol>
 *
 * <p>SQL alternatives fail because:
 *
 * <ul>
 *   <li>{@code SUM() OVER} - produces output for every row, not just threshold crossing
 *   <li>{@code HAVING SUM(price) > threshold} - emits for every qualifying window, not once ever
 *   <li>{@code ROW_NUMBER() WHERE rownum = 1} - can't filter by accumulated state
 * </ul>
 *
 * <h3>Use Cases</h3>
 *
 * <ul>
 *   <li>Loyalty program tier upgrades (emit when customer reaches Gold status)
 *   <li>Fraud alerts (emit when cumulative spend exceeds risk threshold)
 *   <li>Sales milestone notifications (emit when customer crosses $1000 lifetime spend)
 *   <li>Compliance triggers (emit when transaction volume exceeds reporting threshold)
 * </ul>
 *
 * <h3>Features</h3>
 *
 * <ul>
 *   <li><b>SET_SEMANTIC_TABLE only</b>: No advanced argument traits required
 *   <li><b>Stateful threshold detection</b>: Tracks running total + emitted flag
 *   <li><b>Configurable threshold</b>: Pass threshold as scalar argument
 *   <li><b>Append-only output</b>: Emits exactly once per customer when threshold crossed
 *   <li><b>Simple POJO state</b>: No TTL, MapView, or ListView - just plain fields
 * </ul>
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * CREATE FUNCTION SpendThresholdDetector AS 'com.flink.ptf.SpendThresholdDetector';
 *
 * -- Emit when customer crosses $500 cumulative spend
 * SELECT customer_id, total_spend, crossing_order_id, order_count
 * FROM SpendThresholdDetector(
 *     input => TABLE orders PARTITION BY customer_id,
 *     threshold => CAST(500.0 AS DOUBLE),
 *     uid => 'spend-threshold-v1'
 * );
 * </pre>
 */
public class SpendThresholdDetector
    extends ProcessTableFunction<SpendThresholdDetector.ThresholdCrossing> {

  /** Output POJO representing the moment a customer crossed the threshold. */
  public static class ThresholdCrossing {
    public int customer_id;
    public double total_spend;
    public String crossing_order_id;
    public int order_count;
  }

  /** State POJO tracking cumulative spend and whether we've already emitted. */
  public static class SpendState {
    public double totalSpend = 0.0;
    public int orderCount = 0;
    public boolean hasEmitted = false;
  }

  /**
   * Processes each order, emitting only when the customer first crosses the spending threshold.
   *
   * @param state Per-partition state tracking spend and emission status
   * @param input The order row
   * @param threshold The spending threshold that triggers emission (must be CAST to DOUBLE)
   */
  public void eval(
      @StateHint SpendState state,
      @ArgumentHint(SET_SEMANTIC_TABLE) Row input,
      Double threshold) {

    // If we've already emitted for this customer, do nothing
    if (state.hasEmitted) {
      return;
    }

    // Update running totals
    Double price = input.getFieldAs("price");
    state.totalSpend += price;
    state.orderCount++;

    // Check if we just crossed the threshold
    if (state.totalSpend >= threshold) {
      state.hasEmitted = true;

      ThresholdCrossing result = new ThresholdCrossing();
      result.customer_id = input.getFieldAs("customer_id");
      result.total_spend = state.totalSpend;
      result.crossing_order_id = input.getFieldAs("order_id");
      result.order_count = state.orderCount;

      collect(result);
    }
  }
}
