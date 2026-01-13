package com.flink.ptf;

import static org.apache.flink.table.annotation.ArgumentTrait.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * An enhanced fraud detection ProcessTableFunction demonstrating advanced PTF features.
 *
 * <p>This PTF analyzes transaction streams to identify potentially fraudulent activity based on
 * multiple detection rules:
 *
 * <ul>
 *   <li><b>Velocity Check:</b> Flags high transaction frequency (>10 txns/hour)
 *   <li><b>Geographic Impossibility:</b> Detects rapid location changes
 *   <li><b>Amount Anomaly:</b> Identifies transactions significantly above average
 *   <li><b>Merchant Pattern:</b> Tracks unusual merchant frequency
 *   <li><b>Recent Pattern Analysis:</b> Uses ListView to track last N transactions for pattern
 *       detection
 * </ul>
 *
 * <h3>Advanced Features Demonstrated</h3>
 *
 * <ul>
 *   <li><b>ListView:</b> Maintains ordered list of recent transactions
 *   <li><b>MapView:</b> Tracks merchant visit counts
 *   <li><b>POJO Output:</b> Strongly-typed output instead of Row
 *   <li><b>Multiple State TTLs:</b> Different TTLs for different state types (30d, 24h, 7d)
 *   <li><b>Multiple DataView State:</b> Both MapView and ListView in same PTF
 * </ul>
 *
 * <h3>State Management</h3>
 *
 * <p>The PTF maintains three types of state per user:
 *
 * <ul>
 *   <li><b>AnomalyState:</b> Tracks user transaction history (30 day TTL)
 *   <li><b>MerchantCounts:</b> MapView tracking merchant visit frequency (24 hour TTL)
 *   <li><b>RecentTransactions:</b> ListView of last N transactions (7 day TTL)
 * </ul>
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * CREATE FUNCTION AnomalyDetector AS 'com.flink.ptf.AnomalyDetector';
 *
 * SELECT userId, alertType, riskScore, reason, transactionCount
 * FROM AnomalyDetector(
 *     input => TABLE transactions PARTITION BY userId,
 *     on_time => DESCRIPTOR(event_time),
 *     uid => 'anomaly-detector-v3'
 * );
 * </pre>
 */
public class AnomalyDetector extends ProcessTableFunction<AnomalyDetector.FraudAlert> {

  private static final long serialVersionUID = 1L;

  // Configuration constants
  private static final double RISK_THRESHOLD = 0.7;
  private static final int MAX_HISTORY_SIZE = 100;

  /** POJO output type for fraud alerts - demonstrates structured output. */
  public static class FraudAlert {
    public String userId;
    public String alertType;
    public Double riskScore;
    public String reason;
    public Integer transactionCount;
  }

  /** State class tracking user transaction patterns. */
  public static class AnomalyState {
    public Double totalAmount = 0.0;
    public Integer txnCount = 0;
    public String lastCountry = null;
    public Long lastTxnTime = null;
  }

  /** POJO for storing transaction details in ListView. */
  public static class TransactionRecord {
    public Double amount;
    public String country;
    public String merchant;
    public Long timestamp;
  }

  /**
   * Evaluates incoming transactions for anomalous patterns.
   *
   * @param state User transaction state (30 day TTL)
   * @param merchantCounts Map of merchant visit counts (24 hour TTL)
   * @param recentTransactions List of recent transactions (7 day TTL)
   * @param input Transaction row containing: userId, amount, country, merchant, timestamp
   */
  public void eval(
      @StateHint(ttl = "30 days") AnomalyState state,
      @StateHint(ttl = "24 hours") MapView<String, Integer> merchantCounts,
      @StateHint(ttl = "7 days") ListView<TransactionRecord> recentTransactions,
      @ArgumentHint({SET_SEMANTIC_TABLE, REQUIRE_ON_TIME}) Row input)
      throws Exception {

    // Extract transaction fields
    String userId = input.getFieldAs("userId");
    Double amount = input.getFieldAs("amount");
    String country = input.getFieldAs("country");
    String merchant = input.getFieldAs("merchant");
    Long timestamp = input.getFieldAs("timestamp");

    double riskScore = 0.0;
    List<String> reasons = new ArrayList<>();

    // Rule 1: Velocity check (transactions per hour)
    if (state.lastTxnTime != null
        && timestamp - state.lastTxnTime < 3600000
        && state.txnCount > 10) {
      riskScore += 0.3;
      reasons.add("HIGH_VELOCITY");
    }

    // Rule 2: Geographic impossibility
    if (state.lastCountry != null
        && !state.lastCountry.equals(country)
        && state.lastTxnTime != null
        && timestamp - state.lastTxnTime < 7200000) {
      riskScore += 0.4;
      reasons.add("GEO_IMPOSSIBLE");
    }

    // Rule 3: Amount anomaly detection
    if (state.txnCount > 0) {
      double avgAmount = state.totalAmount / state.txnCount;
      if (amount > avgAmount * 5) {
        riskScore += 0.3;
        reasons.add("AMOUNT_ANOMALY");
      }
    }

    // Rule 4: Unusual merchant patterns
    Integer merchantCount = merchantCounts.get(merchant);
    if (merchantCount != null && merchantCount > 5) {
      riskScore += 0.2;
      reasons.add("MERCHANT_FREQUENCY");
    }
    merchantCounts.put(merchant, merchantCount == null ? 1 : merchantCount + 1);

    // Rule 5: Pattern detection using ListView - check for rapid small transactions
    List<TransactionRecord> txnHistory = new ArrayList<>();
    for (TransactionRecord record : recentTransactions.get()) {
      txnHistory.add(record);
    }
    if (txnHistory.size() >= 5) {
      int smallTxnCount = 0;
      for (TransactionRecord record :
          txnHistory.subList(txnHistory.size() - 5, txnHistory.size())) {
        if (record.amount < 10.0) {
          smallTxnCount++;
        }
      }
      if (smallTxnCount >= 4) {
        riskScore += 0.25;
        reasons.add("SMALL_TXN_PATTERN");
      }
    }

    // Add current transaction to history
    TransactionRecord currentTxn = new TransactionRecord();
    currentTxn.amount = amount;
    currentTxn.country = country;
    currentTxn.merchant = merchant;
    currentTxn.timestamp = timestamp;
    recentTransactions.add(currentTxn);

    // Maintain history size limit
    if (txnHistory.size() >= MAX_HISTORY_SIZE) {
      // Remove oldest transaction
      List<TransactionRecord> updated = new ArrayList<>();
      int i = 0;
      for (TransactionRecord record : recentTransactions.get()) {
        if (i > 0) { // Skip first (oldest) element
          updated.add(record);
        }
        i++;
      }
      recentTransactions.clear();
      for (TransactionRecord record : updated) {
        recentTransactions.add(record);
      }
    }

    // Emit alert if risk threshold exceeded
    if (riskScore >= RISK_THRESHOLD) {
      FraudAlert alert = new FraudAlert();
      alert.userId = userId;
      alert.alertType = "HIGH_RISK";
      alert.riskScore = riskScore;
      alert.reason = String.join(",", reasons);
      alert.transactionCount = state.txnCount + 1;
      collect(alert);
    }

    // Update state
    state.totalAmount += amount;
    state.txnCount++;
    state.lastCountry = country;
    state.lastTxnTime = timestamp;
  }
}
