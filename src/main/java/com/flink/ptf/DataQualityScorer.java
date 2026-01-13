package com.flink.ptf;

import static org.apache.flink.table.annotation.ArgumentTrait.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * A ProcessTableFunction that demonstrates PASS_COLUMNS_THROUGH for automatic column forwarding.
 *
 * <p>This PTF validates data quality and adds quality metrics columns while automatically
 * preserving all input columns without manual forwarding.
 *
 * <p><b>Key Feature:</b> Uses {@code ArgumentTrait.PASS_COLUMNS_THROUGH} to automatically include
 * all input columns in the output, eliminating the need to manually forward columns.
 *
 * <h3>Quality Rules</h3>
 *
 * <p>The scorer evaluates multiple data quality dimensions:
 *
 * <ul>
 *   <li><b>Amount Validation:</b> Must be between 0 and 10,000 (20 points)
 *   <li><b>UserId Validation:</b> Must be non-null and 3+ characters (30 points)
 *   <li><b>Country Validation:</b> Must be non-null and 2-3 characters (25 points)
 *   <li><b>Merchant Validation:</b> Must be non-null (25 points)
 * </ul>
 *
 * <h3>Output Schema</h3>
 *
 * <p>Input columns: {@code userId, amount, country, merchant, ts}
 *
 * <p>Output columns (with PASS_COLUMNS_THROUGH): {@code userId, amount, country, merchant, ts,
 * quality_score, quality_issues, is_valid}
 *
 * <p>Added columns:
 *
 * <ul>
 *   <li><b>quality_score:</b> Score from 0.0 to 100.0 (higher is better)
 *   <li><b>quality_issues:</b> Comma-separated list of issues found
 *   <li><b>is_valid:</b> TRUE if score >= 80.0
 * </ul>
 *
 * <h3>PASS_COLUMNS_THROUGH Requirements</h3>
 *
 * <p>This trait has strict requirements:
 *
 * <ul>
 *   <li>✅ Single table argument (no multiple inputs)
 *   <li>✅ Append-only PTF (no updates/deletes)
 *   <li>✅ No timers (stateless or simple state)
 *   <li>✅ Row semantic table (ROW_SEMANTIC_TABLE) - each row processed independently
 * </ul>
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * -- Create source table
 * CREATE TABLE transactions (
 *     userId STRING,
 *     amount DOUBLE,
 *     country STRING,
 *     merchant STRING,
 *     ts TIMESTAMP(3)
 * ) WITH ('connector' = 'datagen', ...);
 *
 * -- Register PTF
 * CREATE FUNCTION DataQualityScorer AS 'com.flink.ptf.DataQualityScorer';
 *
 * -- Execute - all input columns automatically included in output
 * SELECT * FROM DataQualityScorer(
 *     input => TABLE transactions,
 *     uid => 'quality-scorer-v1'
 * );
 * </pre>
 *
 * <h3>Example Output</h3>
 *
 * <pre>
 * userId | amount | country | merchant | ts                  | quality_score | quality_issues          | is_valid
 * -------|--------|---------|----------|---------------------|---------------|-------------------------|----------
 * Alice  | 50.0   | US      | Acme     | 2025-03-27 10:00:00 | 100.0         |                         | TRUE
 * Bob    | -10.0  | X       | null     | 2025-03-27 10:01:00 | 0.0           | INVALID_AMOUNT,INV...   | FALSE
 * Carol  | 500.0  | UK      | Shop     | 2025-03-27 10:02:00 | 100.0         |                         | TRUE
 * </pre>
 *
 * <h3>Use Cases</h3>
 *
 * <ul>
 *   <li>Data quality monitoring and alerting
 *   <li>Input validation for downstream systems
 *   <li>Enrichment with quality metadata
 *   <li>Filtering low-quality records
 *   <li>Data profiling and observability
 * </ul>
 *
 * <h3>Why PASS_COLUMNS_THROUGH?</h3>
 *
 * <p>Without PASS_COLUMNS_THROUGH, you would need to manually include all input columns in the
 * output POJO/Row, leading to:
 *
 * <ul>
 *   <li>Boilerplate code for forwarding columns
 *   <li>Maintenance burden when schema changes
 *   <li>Risk of forgetting to forward columns
 * </ul>
 *
 * <p>With PASS_COLUMNS_THROUGH, the PTF only defines NEW columns, and all input columns are
 * automatically preserved.
 */
@DataTypeHint("ROW<quality_score DOUBLE, quality_issues STRING, is_valid BOOLEAN>")
public class DataQualityScorer extends ProcessTableFunction<Row> {

  // Quality score thresholds
  private static final double AMOUNT_POINTS = 20.0;
  private static final double USERID_POINTS = 30.0;
  private static final double COUNTRY_POINTS = 25.0;
  private static final double MERCHANT_POINTS = 25.0;
  private static final double PASSING_THRESHOLD = 80.0;

  // Validation ranges
  private static final double MIN_AMOUNT = 0.0;
  private static final double MAX_AMOUNT = 10000.0;
  private static final int MIN_USERID_LENGTH = 3;
  private static final int MIN_COUNTRY_LENGTH = 2;
  private static final int MAX_COUNTRY_LENGTH = 3;

  /**
   * Evaluates data quality for each transaction.
   *
   * <p>This method is stateless - each row is processed independently. The PASS_COLUMNS_THROUGH
   * trait ensures all input columns (userId, amount, country, merchant, ts) are automatically
   * included in the output alongside the quality metrics.
   *
   * @param input Transaction row with fields: userId, amount, country, merchant, ts
   */
  public void eval(@ArgumentHint({ROW_SEMANTIC_TABLE, PASS_COLUMNS_THROUGH}) Row input) {

    // Extract fields for validation
    String userId = input.getFieldAs("userId");
    Double amount = input.getFieldAs("amount");
    String country = input.getFieldAs("country");
    String merchant = input.getFieldAs("merchant");

    // Track quality score and issues
    double score = 0.0;
    List<String> issues = new ArrayList<>();

    // Rule 1: Amount validation (20 points)
    if (amount != null && amount >= MIN_AMOUNT && amount <= MAX_AMOUNT) {
      score += AMOUNT_POINTS;
    } else {
      issues.add("INVALID_AMOUNT");
    }

    // Rule 2: UserId validation (30 points)
    if (userId != null && userId.length() >= MIN_USERID_LENGTH) {
      score += USERID_POINTS;
    } else {
      issues.add("INVALID_USERID");
    }

    // Rule 3: Country validation (25 points)
    if (country != null
        && country.length() >= MIN_COUNTRY_LENGTH
        && country.length() <= MAX_COUNTRY_LENGTH) {
      score += COUNTRY_POINTS;
    } else {
      issues.add("INVALID_COUNTRY");
    }

    // Rule 4: Merchant validation (25 points)
    if (merchant != null && !merchant.trim().isEmpty()) {
      score += MERCHANT_POINTS;
    } else {
      issues.add("INVALID_MERCHANT");
    }

    // Determine validity
    boolean isValid = score >= PASSING_THRESHOLD;

    // Build issues string
    String issuesStr = issues.isEmpty() ? "" : String.join(",", issues);

    // Emit quality metrics
    // Note: Input columns (userId, amount, country, merchant, ts) are automatically
    // passed through due to PASS_COLUMNS_THROUGH trait
    collect(Row.of(score, issuesStr, isValid));
  }
}
