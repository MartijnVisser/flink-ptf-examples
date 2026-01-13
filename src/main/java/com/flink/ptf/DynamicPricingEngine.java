package com.flink.ptf;

import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

import org.apache.flink.table.annotation.*;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * An enhanced dynamic pricing engine ProcessTableFunction demonstrating advanced PTF features.
 *
 * <p>This PTF processes two input streams to calculate optimal pricing:
 *
 * <ul>
 *   <li><b>Inventory Updates:</b> Current stock levels (required)
 *   <li><b>Competitor Prices:</b> Market pricing data (optional)
 * </ul>
 *
 * <h3>Advanced Features Demonstrated</h3>
 *
 * <ul>
 *   <li><b>Optional Inputs:</b> Competitor prices are optional - engine works without them (null
 *       checks)
 *   <li><b>POJO Output:</b> Strongly-typed output instead of Row
 *   <li><b>Multiple Input Streams:</b> Processes 2 different event streams
 *   <li><b>Hardcoded Configuration:</b> Constants used instead of scalar arguments (Flink
 *       limitation with multiple tables)
 * </ul>
 *
 * <h3>Pricing Rules</h3>
 *
 * <p>The engine applies these prioritized rules:
 *
 * <ol>
 *   <li><b>Low Inventory:</b> Increase price when stock falls below threshold
 *   <li><b>High Demand:</b> Increase price when demand signals exceed threshold
 *   <li><b>Competitor Matching:</b> Match competitor price (if available) within tolerance
 * </ol>
 *
 * <h3>State Management</h3>
 *
 * <p>Maintains pricing state per SKU with 7-day TTL:
 *
 * <ul>
 *   <li>Current price
 *   <li>Inventory level
 *   <li>Competitor price (optional)
 *   <li>Demand signal count
 *   <li>Last update timestamp
 * </ul>
 *
 * <h3>Configuration</h3>
 *
 * <p>Due to Flink limitations (scalar arguments not supported with multiple table arguments),
 * pricing thresholds are hardcoded as constants:
 *
 * <ul>
 *   <li>MIN_PRICE_CHANGE_PERCENT = 0.02 (2% minimum change to emit)
 *   <li>INVENTORY_THRESHOLD = 10 (low inventory trigger)
 *   <li>DEMAND_THRESHOLD = 100 (high demand trigger)
 * </ul>
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * CREATE FUNCTION DynamicPricingEngine AS 'com.flink.ptf.DynamicPricingEngine';
 *
 * SELECT sku, newPrice, oldPrice, reason, timestamp
 * FROM DynamicPricingEngine(
 *     inventoryEvent => TABLE inventory PARTITION BY sku,
 *     competitorPriceEvent => TABLE competitor_prices PARTITION BY sku,
 *     uid => 'pricing-engine-v9'
 * );
 * </pre>
 */
public class DynamicPricingEngine extends ProcessTableFunction<DynamicPricingEngine.PriceUpdate> {

  /** POJO output type for price updates - demonstrates structured output. */
  public static class PriceUpdate {
    public String sku;
    public Double newPrice;
    public Double oldPrice;
    public String reason;
    public Long timestamp;
  }

  /** State class tracking pricing factors per SKU. */
  public static class PricingState {
    public Double currentPrice = null;
    public Integer inventoryLevel = 0;
    public Double competitorPrice = null;
    public Integer demandSignals = 0;
    public Long lastUpdate = null;
    public Integer demandSignalsSinceReset = 0;
  }

  // Configuration constants (cannot use scalar arguments with multiple table inputs)
  private static final double MIN_PRICE_CHANGE_PERCENT = 0.02;
  private static final int INVENTORY_THRESHOLD = 10;
  private static final int DEMAND_THRESHOLD = 100;

  /**
   * Evaluates pricing changes based on incoming events from multiple streams.
   *
   * @param state Pricing state (7 day TTL)
   * @param inventoryEvent Inventory update with fields: sku, quantity, basePrice (optional - may be
   *     null)
   * @param competitorPriceEvent Competitor price with fields: sku, price (optional - may be null)
   */
  public void eval(
      @StateHint(ttl = "7 days") PricingState state,
      @ArgumentHint(SET_SEMANTIC_TABLE) Row inventoryEvent,
      @ArgumentHint(SET_SEMANTIC_TABLE) Row competitorPriceEvent) {

    String currentSku = null;

    // Process inventory updates
    if (inventoryEvent != null) {
      currentSku = inventoryEvent.getFieldAs("sku");
      state.inventoryLevel = inventoryEvent.getFieldAs("quantity");

      // Track demand signals (each inventory update indicates demand)
      state.demandSignalsSinceReset++;

      // Initialize price if first time seeing this SKU
      if (state.currentPrice == null) {
        state.currentPrice = inventoryEvent.getFieldAs("basePrice");
      }
    }

    // Process competitor price changes (optional input - may be null)
    if (competitorPriceEvent != null) {
      currentSku = competitorPriceEvent.getFieldAs("sku");
      state.competitorPrice = competitorPriceEvent.getFieldAs("price");
    }

    // Note: Demand signals are now tracked via inventory events (above)
    // This allows HIGH_DEMAND pricing rule to trigger after sufficient inventory updates

    // Calculate new price based on multiple factors
    if (currentSku != null && state.currentPrice != null) {
      double newPrice = calculateOptimalPrice(state);
      String reason = null;
      StringBuilder reasonBuilder = new StringBuilder();

      // Apply pricing rules in priority order
      // Rule 1: Low inventory = increase price
      if (state.inventoryLevel < INVENTORY_THRESHOLD && newPrice < state.currentPrice * 1.2) {
        newPrice = state.currentPrice * 1.15;
        reasonBuilder.append("LOW_INVENTORY");
      }

      // Rule 2: High demand = increase price
      if (state.demandSignalsSinceReset > DEMAND_THRESHOLD && newPrice < state.currentPrice * 1.1) {
        newPrice = state.currentPrice * 1.08;
        if (reasonBuilder.length() > 0) reasonBuilder.append(",");
        reasonBuilder.append("HIGH_DEMAND");
        state.demandSignalsSinceReset = 0; // Reset counter after applying rule
      }

      // Rule 3: Competitor pricing = match within 5% (only if competitor data available)
      if (state.competitorPrice != null
          && Math.abs(newPrice - state.competitorPrice) > state.competitorPrice * 0.05) {
        newPrice = state.competitorPrice * 0.98; // Slightly undercut
        if (reasonBuilder.length() > 0) reasonBuilder.append(",");
        reasonBuilder.append("COMPETITOR_MATCH");
      }

      reason = reasonBuilder.length() > 0 ? reasonBuilder.toString() : "BASELINE";

      // Emit price update if significant change (using constant threshold)
      if (Math.abs(newPrice - state.currentPrice) > state.currentPrice * MIN_PRICE_CHANGE_PERCENT) {
        PriceUpdate update = new PriceUpdate();
        update.sku = currentSku;
        update.newPrice = newPrice;
        update.oldPrice = state.currentPrice;
        update.reason = reason;
        update.timestamp = System.currentTimeMillis();

        collect(update);

        state.currentPrice = newPrice;
        state.lastUpdate = System.currentTimeMillis();
      }
    }
  }

  /**
   * Calculates optimal price considering all factors.
   *
   * @param state Current pricing state
   * @return Calculated optimal price
   */
  private double calculateOptimalPrice(PricingState state) {
    // Sophisticated pricing algorithm could consider:
    // - Current inventory levels
    // - Demand signals
    // - Competitor pricing
    // - Historical pricing data
    // - Seasonal trends
    // Simplified for this example
    return state.currentPrice;
  }
}
