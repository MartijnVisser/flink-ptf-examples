package com.flink.ptf;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * An early-firing tumbling window ProcessTableFunction that emits on every incoming event.
 *
 * <p>Unlike traditional tumbling windows that wait for the window to close before emitting, this
 * PTF emits the current window aggregation immediately whenever a new event arrives. This is useful
 * for low-latency use cases where you want to see partial window results as they accumulate.
 *
 * <h3>Important: Running Aggregates</h3>
 *
 * <p>Each emission contains the <b>cumulative aggregation</b> of all events within the current
 * window, not just the single triggering event. For example, if 3 events arrive in a window:
 *
 * <pre>
 * Event 1 → emits: num_orders=1, total_price=10.0
 * Event 2 → emits: num_orders=2, total_price=25.0  (running total)
 * Event 3 → emits: num_orders=3, total_price=40.0  (running total)
 * </pre>
 *
 * <p>This provides progressive refinement of window results as data arrives.
 *
 * <h3>Key Behavior</h3>
 *
 * <ul>
 *   <li><b>Immediate emission:</b> Every event triggers an emission with the current running
 *       aggregate
 *   <li><b>Running aggregates:</b> Each emission shows the cumulative state of the window so far
 *   <li><b>State reset on window boundary:</b> When an event falls into a new window, the previous
 *       state is discarded and aggregation restarts from zero
 * </ul>
 *
 * <h3>Use Cases</h3>
 *
 * <ul>
 *   <li>Real-time dashboards requiring immediate updates with running totals
 *   <li>Alerting systems that need to react to partial aggregations
 *   <li>Progressive result refinement as more data arrives
 * </ul>
 *
 * <h3>Usage Note</h3>
 *
 * <p>The {@code windowSizeMillis} parameter must be passed as {@code CAST(5000 AS BIGINT)} rather
 * than {@code INTERVAL '5' SECONDS} due to a known Flink issue with INTERVAL arguments in PTFs. See
 * <a href="https://issues.apache.org/jira/browse/FLINK-37618">FLINK-37618</a> for details.
 *
 * @see <a href="https://issues.apache.org/jira/browse/FLINK-37618">FLINK-37618: Support PTFs
 *     INTERVAL argument</a>
 */
public class EarlyFireWindowFunction
    extends ProcessTableFunction<EarlyFireWindowFunction.WindowResult> {

  public static class WindowResult {
    public LocalDateTime window_start;
    public LocalDateTime window_end;
    public int num_orders;
    public double total_price;
  }

  public static class WindowState {
    public long windowStartTimestamp = -1L;
    public long windowEndTimestamp = -1L;
    public int count = 0;
    public double priceSum = 0.0;
  }

  public void eval(
      Context ctx,
      @StateHint WindowState state,
      @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.REQUIRE_ON_TIME}) Row input,
      Long windowSizeMillis) {

    TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
    long currentEvtTime = timeCtx.time();

    // Calculate the tumbling window boundaries for this event
    long windowStart = currentEvtTime - (currentEvtTime % windowSizeMillis);
    long windowEnd = windowStart + windowSizeMillis;

    // Check if we've moved to a new window - if so, reset state
    if (state.windowStartTimestamp != windowStart) {
      // New window detected, reset state
      state.windowStartTimestamp = windowStart;
      state.windowEndTimestamp = windowEnd;
      state.count = 0;
      state.priceSum = 0.0;
    }

    // Aggregate the current event
    state.count++;
    Double price = input.getFieldAs("price");
    if (price != null) {
      state.priceSum += price;
    }

    // Immediately emit the current window result
    emitWindowResult(state);
  }

  // Helper: Emit current window results
  private void emitWindowResult(WindowState state) {
    WindowResult result = new WindowResult();
    result.window_start =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(state.windowStartTimestamp), ZoneOffset.UTC);
    result.window_end =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(state.windowEndTimestamp), ZoneOffset.UTC);
    result.num_orders = state.count;
    result.total_price = state.priceSum;

    collect(result);
  }
}
