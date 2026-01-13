package com.flink.ptf;

import static org.apache.flink.table.annotation.ArgumentTrait.*;

import java.time.Duration;
import java.time.Instant;
import org.apache.flink.table.annotation.*;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * An enhanced session tracking ProcessTableFunction demonstrating advanced PTF features.
 *
 * <p>This PTF tracks user sessions by:
 *
 * <ul>
 *   <li>Starting a session on the first event
 *   <li>Extending the session timeout on each subsequent event
 *   <li>Ending the session on explicit LOGOUT events
 *   <li>Automatically timing out inactive sessions after configurable period
 *   <li>Tracking all events within the session for analytics
 * </ul>
 *
 * <h3>Advanced Features Demonstrated</h3>
 *
 * <ul>
 *   <li><b>Scalar Arguments:</b> Configurable session timeout
 *   <li><b>POJO Output:</b> Strongly-typed output instead of Row
 *   <li><b>ListView:</b> Maintains ordered list of session events
 *   <li><b>Timers:</b> Event-time timers for session expiration
 *   <li><b>TimeContext:</b> Event-time context for time-based operations
 * </ul>
 *
 * <h3>Session Lifecycle</h3>
 *
 * <p>Sessions can end in two ways:
 *
 * <ol>
 *   <li><b>Explicit End:</b> User sends a LOGOUT event, emits "COMPLETED" status
 *   <li><b>Timeout:</b> No activity for timeout period, emits "TIMEOUT" status
 * </ol>
 *
 * <h3>State Management</h3>
 *
 * <p>Maintains session state per user with 2-hour TTL:
 *
 * <ul>
 *   <li>firstEventType - Type of the first event in the session
 *   <li>sessionStart - Session start timestamp
 *   <li>userId - User identifier
 *   <li>eventCount - Number of events in session
 *   <li>sessionEvents - ListView of all event types (for analytics)
 * </ul>
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * CREATE FUNCTION SessionTracker AS 'com.flink.ptf.SessionTracker';
 *
 * SELECT userId, status, sessionDuration, eventCount, firstEventType, lastEventType
 * FROM SessionTracker(
 *     input => TABLE user_events PARTITION BY userId,
 *     sessionTimeoutMinutes => 30,
 *     on_time => DESCRIPTOR(event_time),
 *     uid => 'session-tracker-v2'
 * );
 * </pre>
 */
public class SessionTracker extends ProcessTableFunction<SessionTracker.SessionSummary> {

  /** POJO output type for session summaries - demonstrates structured output. */
  public static class SessionSummary {
    public String userId;
    public String status;
    public Long sessionDuration;
    public Integer eventCount;
    public String firstEventType;
    public String lastEventType;
  }

  /** State class tracking user session information. */
  public static class SessionState {
    public String firstEventType = null;
    public String lastEventType = null;
    public Long sessionStart = null;
    public String userId = null;
    public Integer eventCount = 0;
  }

  /**
   * Evaluates incoming user events and manages session state.
   *
   * @param ctx Process context
   * @param state Session state (2 hour TTL)
   * @param sessionEvents List of event types in session (2 hour TTL)
   * @param input Event row containing: userId, eventType
   * @param sessionTimeoutSeconds Configurable session timeout in seconds (scalar argument)
   */
  public void eval(
      Context ctx,
      @StateHint(ttl = "2 hours") SessionState state,
      @StateHint(ttl = "2 hours") ListView<String> sessionEvents,
      @ArgumentHint({SET_SEMANTIC_TABLE, REQUIRE_ON_TIME}) Row input,
      Integer sessionTimeoutSeconds)
      throws Exception {

    TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
    String userId = input.getFieldAs("userId");
    String eventType = input.getFieldAs("eventType");

    if (state.firstEventType == null) {
      // First event in session
      state.firstEventType = eventType;
      state.sessionStart = timeCtx.time().toEpochMilli();
      state.userId = userId;
      state.eventCount = 1;

      // Set configurable session timeout
      timeCtx.registerOnTime(
          "timeout", timeCtx.time().plus(Duration.ofSeconds(sessionTimeoutSeconds)));
    } else {
      // Subsequent event: increment count and extend session
      state.eventCount++;
      timeCtx.registerOnTime(
          "timeout", timeCtx.time().plus(Duration.ofSeconds(sessionTimeoutSeconds)));
    }

    // Track event type
    state.lastEventType = eventType;
    sessionEvents.add(eventType);

    if ("LOGOUT".equals(eventType)) {
      // Explicit session end
      long duration = timeCtx.time().toEpochMilli() - state.sessionStart;

      SessionSummary summary = new SessionSummary();
      summary.userId = userId;
      summary.status = "COMPLETED";
      summary.sessionDuration = duration;
      summary.eventCount = state.eventCount;
      summary.firstEventType = state.firstEventType;
      summary.lastEventType = state.lastEventType;

      collect(summary);

      // Clear all state and timers
      ctx.clearAll();
    }
  }

  /**
   * Timer callback invoked when session timeout is reached.
   *
   * @param onTimerCtx Timer context
   * @param state Session state
   * @param sessionEvents List of session events
   */
  public void onTimer(OnTimerContext onTimerCtx, SessionState state, ListView<String> sessionEvents)
      throws Exception {
    // Session timeout - emit timeout event
    if (state.sessionStart != null) {
      Instant currentTime = onTimerCtx.timeContext(Instant.class).time();
      long duration = currentTime.toEpochMilli() - state.sessionStart;

      SessionSummary summary = new SessionSummary();
      summary.userId = state.userId != null ? state.userId : "unknown";
      summary.status = "TIMEOUT";
      summary.sessionDuration = duration;
      summary.eventCount = state.eventCount;
      summary.firstEventType = state.firstEventType;
      summary.lastEventType = state.lastEventType;

      collect(summary);

      // State will be cleared automatically after TTL
    }
  }
}
