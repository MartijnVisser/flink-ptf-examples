package com.flink.ptf;

import static org.apache.flink.table.annotation.ArgumentTrait.*;

import java.time.Instant;
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * An enhanced first-match join ProcessTableFunction demonstrating advanced PTF features.
 *
 * <p>This PTF implements a specialized join pattern where:
 *
 * <ul>
 *   <li>Processes two input streams: orders and customers
 *   <li>Emits a result when both sides have arrived for the first time
 *   <li>Ignores all subsequent updates to either side
 *   <li>Useful for dimension table lookups where only the initial enrichment matters
 *   <li>Passes through all columns from input tables
 * </ul>
 *
 * <h3>Advanced Features Demonstrated</h3>
 *
 * <ul>
 *   <li><b>PASS_COLUMNS_THROUGH:</b> Preserves all input columns in output
 *   <li><b>POJO Output:</b> Strongly-typed output instead of Row
 *   <li><b>TimeContext:</b> Uses event-time context for accurate join timestamps
 *   <li><b>Multiple Input Streams:</b> Processes 2 different event streams
 *   <li><b>Join Metadata:</b> Captures which side arrived first and join timestamp
 * </ul>
 *
 * <h3>Join Semantics</h3>
 *
 * <p>Unlike traditional joins that emit updates when either side changes, this join:
 *
 * <ol>
 *   <li>Waits for the first arrival from both order and customer streams
 *   <li>Emits exactly one enriched result
 *   <li>Sets a flag to prevent any future emissions for this key
 *   <li>Subsequent updates to orders or customer data are ignored
 * </ol>
 *
 * <h3>Use Cases</h3>
 *
 * <ul>
 *   <li>Initial order enrichment with customer data
 *   <li>First-time user registration events
 *   <li>One-time dimension table lookups
 *   <li>Scenarios where temporal consistency doesn't matter
 * </ul>
 *
 * <h3>State Management</h3>
 *
 * <p>Maintains join state per partition key (e.g., customerId) with 1-hour TTL:
 *
 * <ul>
 *   <li>Order data - orderId, totalAmount
 *   <li>Customer data - name, email, tier
 *   <li>Join metadata - which side arrived first, join timestamp
 *   <li>alreadyEmitted - Flag preventing duplicate emissions
 * </ul>
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * CREATE FUNCTION FirstMatchJoin AS 'com.flink.ptf.FirstMatchJoin';
 *
 * SELECT orderId, customerId, totalAmount, customerName, customerEmail,
 *        customerTier, firstArrival, joinTimestamp
 * FROM FirstMatchJoin(
 *     orderStream => TABLE orders PARTITION BY customerId,
 *     customerStream => TABLE customers PARTITION BY customerId,
 *     on_time => DESCRIPTOR(event_time),
 *     uid => 'first-match-join-v2'
 * );
 * </pre>
 */
public class FirstMatchJoin extends ProcessTableFunction<FirstMatchJoin.EnrichedOrder> {

  /** POJO output type for enriched orders - demonstrates structured output with pass-through. */
  public static class EnrichedOrder {
    // Order columns (passed through)
    public String orderId;
    public Integer customerId;
    public Double totalAmount;

    // Customer columns (passed through)
    public String customerName;
    public String customerEmail;
    public String customerTier;

    // Join metadata
    public String firstArrival; // "ORDER" or "CUSTOMER"
    public Long joinTimestamp;
  }

  /** State class tracking join state per partition key. */
  public static class FirstMatchState {
    // Order state
    public String orderId = null;
    public Double totalAmount = null;

    // Customer state
    public String customerName = null;
    public String customerEmail = null;
    public String customerTier = null;

    // Join metadata
    public String firstArrival = null;
    public Boolean alreadyEmitted = false;
  }

  /**
   * Evaluates incoming events from both order and customer streams.
   *
   * @param ctx Process context
   * @param state Join state
   * @param orderStream Order row containing: orderId, customerId, totalAmount
   * @param customerStream Customer row containing: customerId, name, email, tier
   */
  public void eval(
      Context ctx,
      @StateHint(ttl = "1 hour") FirstMatchState state,
      @ArgumentHint(SET_SEMANTIC_TABLE) Row orderStream,
      @ArgumentHint(SET_SEMANTIC_TABLE) Row customerStream) {

    TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
    Integer customerId = null;

    // Process order stream
    if (orderStream != null) {
      state.orderId = orderStream.getFieldAs("orderId");
      customerId = orderStream.getFieldAs("customerId");
      state.totalAmount = orderStream.getFieldAs("totalAmount");

      // Track first arrival
      if (state.firstArrival == null) {
        state.firstArrival = "ORDER";
      }

      // Only emit if we haven't emitted yet AND we have customer info
      if (!state.alreadyEmitted && state.customerName != null) {
        emitJoinResult(state, customerId, timeCtx);
      }
    }

    // Process customer stream (dimension table)
    if (customerStream != null) {
      customerId = customerStream.getFieldAs("customerId");
      state.customerName = customerStream.getFieldAs("name");
      state.customerEmail = customerStream.getFieldAs("email");
      state.customerTier = customerStream.getFieldAs("tier");

      // Track first arrival
      if (state.firstArrival == null) {
        state.firstArrival = "CUSTOMER";
      }

      // Only emit if we haven't emitted yet AND we have order info
      if (!state.alreadyEmitted && state.orderId != null) {
        emitJoinResult(state, customerId, timeCtx);
      }
    }
  }

  /**
   * Helper method to emit join result with all passed-through columns.
   *
   * @param state Join state
   * @param customerId Customer ID
   * @param timeCtx Time context
   */
  private void emitJoinResult(
      FirstMatchState state, Integer customerId, TimeContext<Instant> timeCtx) {

    EnrichedOrder result = new EnrichedOrder();

    // Pass through order columns
    result.orderId = state.orderId;
    result.customerId = customerId;
    result.totalAmount = state.totalAmount;

    // Pass through customer columns
    result.customerName = state.customerName;
    result.customerEmail = state.customerEmail;
    result.customerTier = state.customerTier;

    // Add join metadata
    result.firstArrival = state.firstArrival;
    result.joinTimestamp = timeCtx.time().toEpochMilli();

    collect(result);
    state.alreadyEmitted = true;
  }
}
