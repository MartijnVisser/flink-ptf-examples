package com.flink.ptf;

import static org.apache.flink.table.annotation.ArgumentTrait.*;

import java.util.Map;
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * A ProcessTableFunction that converts append-only DynamoDB CDC events into a proper Flink
 * changelog stream.
 *
 * <p>This PTF demonstrates the inverse of {@link ChangelogAuditor}:
 *
 * <ul>
 *   <li><b>ChangelogAuditor</b>: changelog input → append-only output
 *   <li><b>DynamoDBChangelogEmitter</b>: append-only input → changelog output
 * </ul>
 *
 * <h3>Key Features</h3>
 *
 * <ul>
 *   <li><b>ChangelogFunction Interface</b>: Declares retract output mode {@code {+I, -U, +U, -D}}
 *   <li><b>Row.ofKind()</b>: Emits rows with explicit RowKind
 *   <li><b>DynamoDB Type Parsing</b>: Extracts values from {@code {"S": "...", "N": "..."}}
 *       wrappers
 *   <li><b>Generic Schema</b>: Works with any DynamoDB table structure
 * </ul>
 *
 * <h3>DynamoDB CDC Envelope</h3>
 *
 * <p>DynamoDB Streams produce events with this structure:
 *
 * <pre>
 * {
 *   "eventID": "abc123",
 *   "eventName": "INSERT|MODIFY|REMOVE",
 *   "dynamodb": {
 *     "Keys": {"pk": {"S": "user#123"}, "sk": {"S": "profile"}},
 *     "NewImage": {"pk": {"S": "..."}, "name": {"S": "Alice"}, "age": {"N": "30"}},
 *     "OldImage": {"pk": {"S": "..."}, "name": {"S": "Alice"}, "age": {"N": "29"}},
 *     "SequenceNumber": "111",
 *     "SizeBytes": 123,
 *     "StreamViewType": "NEW_AND_OLD_IMAGES"
 *   }
 * }
 * </pre>
 *
 * <h3>Event Mapping (Retract Mode)</h3>
 *
 * <table>
 *   <tr><th>DynamoDB eventName</th><th>Flink RowKind</th></tr>
 *   <tr><td>INSERT</td><td>+I (INSERT)</td></tr>
 *   <tr><td>MODIFY</td><td>-U (UPDATE_BEFORE with OldImage) then +U (UPDATE_AFTER with NewImage)</td></tr>
 *   <tr><td>REMOVE</td><td>-D (DELETE)</td></tr>
 * </table>
 *
 * <p><b>Note:</b> Retract mode is used because Flink's debugging sinks (like SQL client collect())
 * operate in retract mode. Upsert mode PTFs must be tested with upserting sinks like kafka-upsert.
 *
 * <h3>Output Schema</h3>
 *
 * <p>Generic output schema that works with any DynamoDB table:
 *
 * <ul>
 *   <li><b>pk:</b> Partition key value (STRING)
 *   <li><b>sk:</b> Sort key value, if present (STRING)
 *   <li><b>attributes_json:</b> All non-key attributes as JSON (STRING)
 *   <li><b>sequence_number:</b> DynamoDB sequence number for ordering (STRING)
 *   <li><b>event_time:</b> Processing timestamp in milliseconds (BIGINT)
 * </ul>
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * -- Create source table for DynamoDB CDC events
 * CREATE TABLE dynamodb_cdc (
 *     eventID STRING,
 *     eventName STRING,
 *     dynamodb ROW&lt;
 *         Keys MAP&lt;STRING, ROW&lt;S STRING, N STRING&gt;&gt;,
 *         NewImage MAP&lt;STRING, ROW&lt;S STRING, N STRING&gt;&gt;,
 *         OldImage MAP&lt;STRING, ROW&lt;S STRING, N STRING&gt;&gt;,
 *         SequenceNumber STRING,
 *         SizeBytes BIGINT,
 *         StreamViewType STRING
 *     &gt;,
 *     pk AS dynamodb.Keys['pk'].S
 * ) WITH (
 *     'connector' = 'filesystem',
 *     'path' = '/opt/flink/test-data/dynamodb/events.json',
 *     'format' = 'json'
 * );
 *
 * CREATE FUNCTION DynamoDBChangelogEmitter AS 'com.flink.ptf.DynamoDBChangelogEmitter';
 *
 * -- Query emits changelog with proper RowKind (+I, +U, -D)
 * SELECT pk, sk, attributes_json, sequence_number, event_time
 * FROM DynamoDBChangelogEmitter(
 *     input =&gt; TABLE dynamodb_cdc PARTITION BY pk,
 *     uid =&gt; 'dynamodb-changelog-v1'
 * );
 * </pre>
 *
 * <h3>Use Cases</h3>
 *
 * <ul>
 *   <li>Converting DynamoDB Streams to Flink changelog for downstream processing
 *   <li>Building materialized views from DynamoDB CDC
 *   <li>Replicating DynamoDB tables to other systems via Flink
 *   <li>Demonstrating ChangelogFunction interface and Row.ofKind() API
 * </ul>
 */
@DataTypeHint(
    "ROW<pk STRING, sk STRING, attributes_json STRING, sequence_number STRING, event_time BIGINT>")
public class DynamoDBChangelogEmitter extends ProcessTableFunction<Row>
    implements ChangelogFunction {

  /** State class required for ChangelogFunction PTFs. */
  public static class EmitterState {
    // Tracks if we've seen any events for this partition key
    public boolean initialized = false;
  }

  /**
   * Declares that this PTF emits retract mode changelog: INSERT, UPDATE_BEFORE, UPDATE_AFTER,
   * DELETE.
   */
  @Override
  public ChangelogMode getChangelogMode(ChangelogFunction.ChangelogContext ctx) {
    // Use ChangelogMode.all() for retract mode - required for SQL client debugging
    return ChangelogMode.all();
  }

  /**
   * Processes DynamoDB CDC events and emits changelog rows with proper RowKind.
   *
   * <p>For MODIFY events, emits both -U (UPDATE_BEFORE with OldImage) and +U (UPDATE_AFTER with
   * NewImage) as required by retract mode.
   *
   * @param state Per-partition state (required for ChangelogFunction)
   * @param input The DynamoDB CDC event containing eventName and dynamodb envelope
   */
  public void eval(@StateHint EmitterState state, @ArgumentHint(SET_SEMANTIC_TABLE) Row input) {
    state.initialized = true;

    String eventName = input.getFieldAs("eventName");
    Row dynamodb = input.getFieldAs("dynamodb");
    String sequenceNumber = dynamodb.getFieldAs("SequenceNumber");

    // Extract pk from computed column
    String pk = input.getFieldAs("pk");

    // Parse Keys for sk
    @SuppressWarnings("unchecked")
    Map<String, Row> keys = dynamodb.getFieldAs("Keys");
    String sk = keys.get("sk") != null ? extractValue(keys.get("sk")) : null;

    // Get current timestamp for all emissions from this event
    long eventTime = System.currentTimeMillis();

    // Process based on event type
    switch (eventName) {
      case "INSERT":
        {
          @SuppressWarnings("unchecked")
          Map<String, Row> newImage = dynamodb.getFieldAs("NewImage");
          String attributesJson = imageToJson(newImage);
          collect(Row.ofKind(RowKind.INSERT, pk, sk, attributesJson, sequenceNumber, eventTime));
          break;
        }
      case "MODIFY":
        {
          // Retract mode: emit -U (old) then +U (new)
          @SuppressWarnings("unchecked")
          Map<String, Row> oldImage = dynamodb.getFieldAs("OldImage");
          @SuppressWarnings("unchecked")
          Map<String, Row> newImage = dynamodb.getFieldAs("NewImage");

          // Emit UPDATE_BEFORE with OldImage
          String oldAttributesJson = imageToJson(oldImage);
          collect(
              Row.ofKind(
                  RowKind.UPDATE_BEFORE, pk, sk, oldAttributesJson, sequenceNumber, eventTime));

          // Emit UPDATE_AFTER with NewImage
          String newAttributesJson = imageToJson(newImage);
          collect(
              Row.ofKind(
                  RowKind.UPDATE_AFTER, pk, sk, newAttributesJson, sequenceNumber, eventTime));
          break;
        }
      case "REMOVE":
        {
          // For deletes, use OldImage if available, else just keys
          @SuppressWarnings("unchecked")
          Map<String, Row> oldImage = dynamodb.getFieldAs("OldImage");
          if (oldImage == null) {
            oldImage = keys;
          }
          String attributesJson = imageToJson(oldImage);
          collect(Row.ofKind(RowKind.DELETE, pk, sk, attributesJson, sequenceNumber, eventTime));
          break;
        }
      default:
        // Unknown event type, skip
        return;
    }
  }

  /**
   * Extracts the actual value from a DynamoDB type wrapper.
   *
   * <p>DynamoDB uses wrappers like {"S": "value"} for strings and {"N": "123"} for numbers.
   *
   * @param typeWrapper The Row containing S and/or N fields
   * @return The extracted value as a String, or null if not found
   */
  private String extractValue(Row typeWrapper) {
    if (typeWrapper == null) {
      return null;
    }
    // Try S (String) first, then N (Number)
    String s = typeWrapper.getFieldAs("S");
    if (s != null) {
      return s;
    }
    return typeWrapper.getFieldAs("N");
  }

  /**
   * Converts a DynamoDB image (map of attribute name to type wrapper) to a JSON string.
   *
   * <p>Excludes pk and sk since those are emitted as separate columns.
   *
   * @param image The map of attribute names to DynamoDB type wrappers
   * @return JSON string representation of non-key attributes
   */
  private String imageToJson(Map<String, Row> image) {
    if (image == null || image.isEmpty()) {
      return "{}";
    }

    StringBuilder sb = new StringBuilder("{");
    boolean first = true;

    for (Map.Entry<String, Row> entry : image.entrySet()) {
      String key = entry.getKey();
      // Skip key columns - they're emitted separately
      if (key.equals("pk") || key.equals("sk")) {
        continue;
      }

      if (!first) {
        sb.append(",");
      }

      String value = extractValue(entry.getValue());
      // Escape quotes in values for valid JSON
      if (value != null) {
        value = value.replace("\"", "\\\"");
      }
      sb.append("\"").append(key).append("\":\"").append(value).append("\"");
      first = false;
    }

    sb.append("}");
    return sb.toString();
  }
}
