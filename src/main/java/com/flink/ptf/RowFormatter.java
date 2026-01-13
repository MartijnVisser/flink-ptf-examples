package com.flink.ptf;

import static org.apache.flink.table.annotation.ArgumentTrait.*;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * A ProcessTableFunction that demonstrates polymorphic table arguments by accepting tables with ANY
 * schema.
 *
 * <p>This PTF works with any input table, dynamically inspecting the schema at runtime using
 * Context APIs to format rows as structured strings. It demonstrates how to build truly generic
 * PTFs that adapt to different table structures.
 *
 * <p><b>Key Feature:</b> Uses polymorphic table arguments (Row without {@code @DataTypeHint}) to
 * accept tables of any schema. The actual schema is discovered at runtime via {@code
 * Context.tableSemanticsFor()}.
 *
 * <h3>Polymorphic Behavior</h3>
 *
 * <p>The function accepts ANY table and formats each row by:
 *
 * <ul>
 *   <li>Detecting partition key columns via Context
 *   <li>Separating key columns from non-key columns
 *   <li>Formatting as: {@code [keys] => {non-keys}}
 *   <li>Tracking unique row signatures to count distinct patterns per partition
 * </ul>
 *
 * <h3>Output Schema</h3>
 *
 * <p>Output columns:
 *
 * <ul>
 *   <li><b>formatted_row:</b> String representation: {@code [key1=v1, key2=v2] => {col1=v1,
 *       col2=v2, ...}}
 *   <li><b>schema_info:</b> Table schema as string (e.g., "name:STRING, age:INT, score:DOUBLE")
 *   <li><b>distinct_patterns:</b> Count of distinct row patterns seen in this partition
 * </ul>
 *
 * <h3>Usage Examples</h3>
 *
 * <pre>
 * -- Example 1: Format a simple user table
 * CREATE TABLE users (id INT, name STRING, age INT) WITH ('connector' = 'datagen', ...);
 * CREATE FUNCTION RowFormatter AS 'com.flink.ptf.RowFormatter';
 *
 * SELECT * FROM RowFormatter(
 *     input => TABLE users PARTITION BY id,
 *     uid => 'formatter-v1'
 * );
 * -- Output: formatted_row = "[id=1] => {name=Alice, age=30}", schema_info = "id:INT, name:STRING, age:INT", distinct_patterns = 1
 *
 * -- Example 2: Format a complex order table with different schema
 * CREATE TABLE orders (order_id STRING, customer_id INT, product_id STRING, price DOUBLE, ts TIMESTAMP(3))
 *   WITH ('connector' = 'datagen', ...);
 *
 * SELECT * FROM RowFormatter(
 *     input => TABLE orders PARTITION BY customer_id,
 *     uid => 'formatter-v2'
 * );
 * -- Output: formatted_row = "[customer_id=42] => {order_id=ORD-123, product_id=PROD-456, price=99.99, ts=2025-...}", ...
 *
 * -- Example 3: Works with ANY table - completely polymorphic
 * CREATE TABLE metrics (metric_name STRING, value DOUBLE, unit STRING, ts TIMESTAMP(3))
 *   WITH ('connector' = 'datagen', ...);
 *
 * SELECT * FROM RowFormatter(
 *     input => TABLE metrics PARTITION BY metric_name,
 *     uid => 'formatter-v3'
 * );
 * </pre>
 *
 * <h3>How Polymorphic Arguments Work</h3>
 *
 * <p>Key differences from typed arguments:
 *
 * <pre>
 * // NON-polymorphic: Explicit schema required
 * public void eval(@ArgumentHint(SET_SEMANTIC_TABLE) @DataTypeHint("ROW<id INT, name STRING>") Row input) {
 *   // Only works with tables matching this exact schema
 * }
 *
 * // POLYMORPHIC: No type hint, accepts ANY schema
 * public void eval(Context ctx, @ArgumentHint(SET_SEMANTIC_TABLE) Row input) {
 *   // Works with ANY table schema
 *   var semantics = ctx.tableSemanticsFor("input");
 *   DataType actualSchema = semantics.dataType(); // Discover schema at runtime
 * }
 * </pre>
 *
 * <h3>State Management</h3>
 *
 * <p>Maintains per-partition state with 24-hour TTL:
 *
 * <ul>
 *   <li><b>seenPatterns:</b> MapView tracking unique row signatures to count distinct patterns
 * </ul>
 *
 * <h3>Use Cases</h3>
 *
 * <ul>
 *   <li>Generic logging/debugging tools that work with any table
 *   <li>Schema-agnostic monitoring and observability
 *   <li>Building reusable PTF libraries that adapt to different schemas
 *   <li>Data profiling and exploration without hardcoding schemas
 *   <li>Teaching tool for understanding Context APIs and runtime schema inspection
 * </ul>
 *
 * <h3>Why Polymorphic Arguments?</h3>
 *
 * <p>Without polymorphic arguments, you would need to:
 *
 * <ul>
 *   <li>Create separate PTF implementations for each table schema
 *   <li>Hardcode field names and types in the eval() method
 *   <li>Recompile and redeploy when schemas change
 * </ul>
 *
 * <p>With polymorphic arguments, a single PTF works with ANY table, discovering the schema
 * dynamically at runtime.
 */
@DataTypeHint("ROW<formatted_row STRING, schema_info STRING, distinct_patterns BIGINT>")
public class RowFormatter extends ProcessTableFunction<Row> {

  /**
   * Processes rows from ANY table, formatting them dynamically based on runtime schema inspection.
   *
   * <p>This method demonstrates polymorphic behavior:
   *
   * <ul>
   *   <li>No {@code @DataTypeHint} on the Row parameter → accepts any schema
   *   <li>Uses Context to discover partition keys and schema at runtime
   *   <li>Dynamically formats rows based on actual structure
   * </ul>
   *
   * @param ctx Context for accessing table semantics and schema information
   * @param seenPatterns MapView to track distinct row patterns per partition
   * @param input Row from ANY table (polymorphic - schema discovered at runtime)
   */
  public void eval(
      Context ctx,
      @StateHint(ttl = "24 hours") MapView<String, Boolean> seenPatterns,
      @ArgumentHint(SET_SEMANTIC_TABLE) Row input)
      throws Exception {

    // === STEP 1: Discover schema at runtime ===
    var semantics = ctx.tableSemanticsFor("input");

    // Get partition key column indices
    int[] partitionKeyIndices = semantics.partitionByColumns();
    List<Integer> keyPositions =
        Arrays.stream(partitionKeyIndices).boxed().collect(Collectors.toList());

    // Get data type information (works with any schema)
    DataType dataType = semantics.dataType();

    // === STEP 2: Build schema info string ===
    // Extract field names and types dynamically
    String schemaInfo = buildSchemaInfo(input);

    // === STEP 3: Format row as "[keys] => {non-keys}" ===
    String keysPart =
        IntStream.range(0, input.getArity())
            .filter(keyPositions::contains)
            .mapToObj(pos -> getFieldName(input, pos) + "=" + formatValue(input.getField(pos)))
            .collect(Collectors.joining(", "));

    String nonKeysPart =
        IntStream.range(0, input.getArity())
            .filter(pos -> !keyPositions.contains(pos))
            .mapToObj(pos -> getFieldName(input, pos) + "=" + formatValue(input.getField(pos)))
            .collect(Collectors.joining(", "));

    String formattedRow = "[" + keysPart + "] => {" + nonKeysPart + "}";

    // === STEP 4: Track distinct row patterns ===
    // Create a signature based on non-null field positions
    String rowSignature = buildRowSignature(input);
    if (seenPatterns.get(rowSignature) == null) {
      seenPatterns.put(rowSignature, true);
    }

    // Count distinct patterns
    long distinctPatterns = 0;
    for (String key : seenPatterns.keys()) {
      distinctPatterns++;
    }

    // === STEP 5: Emit formatted result ===
    collect(Row.of(formattedRow, schemaInfo, distinctPatterns));
  }

  /**
   * Builds a schema info string dynamically from the row structure.
   *
   * <p>Example output: "id:INT, name:STRING, age:INT, score:DOUBLE"
   */
  private String buildSchemaInfo(Row input) {
    return IntStream.range(0, input.getArity())
        .mapToObj(
            pos -> {
              String fieldName = getFieldName(input, pos);
              Object value = input.getField(pos);
              String typeName = (value != null) ? value.getClass().getSimpleName() : "NULL";
              return fieldName + ":" + typeName;
            })
        .collect(Collectors.joining(", "));
  }

  /**
   * Builds a signature for the row based on field names and null/non-null pattern.
   *
   * <p>Used to track distinct row patterns within a partition.
   */
  private String buildRowSignature(Row input) {
    return IntStream.range(0, input.getArity())
        .mapToObj(
            pos -> {
              String fieldName = getFieldName(input, pos);
              boolean isNull = input.getField(pos) == null;
              return fieldName + ":" + (isNull ? "NULL" : "SET");
            })
        .collect(Collectors.joining("|"));
  }

  /** Formats a field value for display, handling nulls and truncating long strings. */
  private String formatValue(Object value) {
    if (value == null) {
      return "null";
    }
    String str = value.toString();
    // Truncate long values for readability
    if (str.length() > 50) {
      return str.substring(0, 47) + "...";
    }
    return str;
  }

  /**
   * Gets the field name for a given position.
   *
   * <p>Rows can have named fields; if not, uses positional naming.
   */
  private String getFieldName(Row input, int pos) {
    // Try to get field name from Row metadata
    Set<String> fieldNamesSet = input.getFieldNames(false);
    if (fieldNamesSet != null && !fieldNamesSet.isEmpty()) {
      // Convert Set to List for indexed access
      List<String> fieldNames = fieldNamesSet.stream().collect(Collectors.toList());
      if (pos < fieldNames.size() && fieldNames.get(pos) != null) {
        return fieldNames.get(pos);
      }
    }
    // Fallback to positional naming
    return "field_" + pos;
  }
}
