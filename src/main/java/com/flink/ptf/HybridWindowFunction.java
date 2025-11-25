package com.flink.ptf;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * A hybrid tumbling window ProcessTableFunction that triggers based on whichever comes first:
 *
 * <ul>
 *   <li><b>Time-based:</b> when the tumbling window ends (timer fires at window boundary)
 *   <li><b>Count-based:</b> when the count threshold is reached
 * </ul>
 *
 * <h3>Important Behavior: Multiple Emissions Per Window Period</h3>
 *
 * <p>When the count threshold triggers an early emission, the state resets and a new aggregation
 * begins. If more events arrive within the same wall-clock tumbling window period, they will be
 * aggregated into a new window with the <b>same {@code window_start} and {@code window_end}
 * timestamps</b> but different aggregated values.
 *
 * <p>This is by design: windows are aligned to epoch-based tumbling boundaries, but each partition
 * (e.g., per customer) can emit multiple times within the same time period if the count threshold
 * is exceeded. Downstream consumers should be aware that multiple rows may share identical window
 * boundaries.
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
public class HybridWindowFunction extends ProcessTableFunction<HybridWindowFunction.WindowResult> {

  public static class WindowResult {
    public LocalDateTime window_start;
    public LocalDateTime window_end;
    public int num_orders;
    public double total_price;
  }

  public static class WindowState {
    public long windowEndTimestamp = -1L;
    public int count = 0;
    public double priceSum = 0.0;
    public long windowSizeMillis = 0L;
  }

  public void eval(
      Context ctx,
      @StateHint WindowState state,
      @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.REQUIRE_ON_TIME}) Row input,
      Long windowSizeMillis, // Input as BIGINT (milliseconds)
      Integer countThreshold) {
    TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
    long currentEvtTime = timeCtx.time();

    // Initialize Window if necessary
    if (state.windowEndTimestamp == -1L) {
      // Calculate Tumbling Window Alignment
      long windowStart = currentEvtTime - (currentEvtTime % windowSizeMillis);
      long windowEnd = windowStart + windowSizeMillis;

      state.windowEndTimestamp = windowEnd;
      // Save config to state so onTimer can use it
      state.windowSizeMillis = windowSizeMillis;

      // Register timer
      timeCtx.registerOnTime(windowEnd);
    }

    // Aggregate
    state.count++;
    Double price = input.getFieldAs("price");
    if (price != null) {
      state.priceSum += price;
    }

    // Trigger: Count Threshold
    if (state.count >= countThreshold) {
      emitWindowResult(state); // Emit

      // Clean up timer to prevent double firing
      timeCtx.clearTimer(state.windowEndTimestamp);

      // Reset State
      resetState(state);
    }
  }

  // 3. The Timer Method
  public void onTimer(OnTimerContext ctx, WindowState state) throws Exception {
    // Emit result on timeout
    emitWindowResult(state);

    // Reset State
    resetState(state);
  }

  // Helper: Emit results
  private void emitWindowResult(WindowState state) {
    long end = state.windowEndTimestamp;
    // Use the saved size from state to calculate start
    long start = end - state.windowSizeMillis;

    WindowResult result = new WindowResult();
    result.window_start = LocalDateTime.ofInstant(Instant.ofEpochMilli(start), ZoneOffset.UTC);
    result.window_end = LocalDateTime.ofInstant(Instant.ofEpochMilli(end), ZoneOffset.UTC);
    result.num_orders = state.count;
    result.total_price = state.priceSum;

    collect(result);
  }

  // Helper: Clear state
  private void resetState(WindowState state) {
    state.windowEndTimestamp = -1L;
    state.count = 0;
    state.priceSum = 0.0;
    state.windowSizeMillis = 0L;
  }
}
