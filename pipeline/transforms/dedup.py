import apache_beam as beam
import time
from apache_beam.coders import PickleCoder
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
    TimeDomain,
)
from datetime import datetime
from pipeline.observability.metrics import PipelineMetrics

def _parse_event_ts(event):
    try:
        ts = event.get("event_ts")
        if not ts:
            return 0.0
        # Handles 'Z' and ISO offsets
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0

class _BufferedDedupDoFn(beam.DoFn):
    """
    Stateful logic to keep only the newest event version within a processing-time window.
    """
    # Stores the actual event dictionary
    latest_event_state = ReadModifyWriteStateSpec("latest_event", PickleCoder())
    # Stores the float timestamp of the current buffered event
    latest_ts_state = ReadModifyWriteStateSpec("latest_ts", beam.coders.FloatCoder())
    # Real-time timer for the buffer duration
    emit_timer = TimerSpec("emit", TimeDomain.REAL_TIME)

    def __init__(self, buffer_seconds: int, max_state_age_sec: int):
        self.buffer_seconds = buffer_seconds
        self.max_state_age_sec = max_state_age_sec

    def process(
        self,
        element,
        event_state=beam.DoFn.StateParam(latest_event_state),
        ts_state=beam.DoFn.StateParam(latest_ts_state),
        timer=beam.DoFn.TimerParam(emit_timer),
    ):
        key, event = element
        PipelineMetrics.dedup_seen.inc()

        new_ts = _parse_event_ts(event)
        existing_ts = ts_state.read() or -1.0

        # TTL Check: If the state is older than max_state_age, treat as new
        if existing_ts > 0 and (new_ts - existing_ts) > self.max_state_age_sec:
            event_state.clear()
            ts_state.clear()
            existing_ts = -1.0

        # Optimization: Only update state if the incoming event is strictly newer
        if new_ts > existing_ts:
            event_state.write(event)
            ts_state.write(new_ts)
            
            # Snooze Logic: Push the timer forward from CURRENT clock time.
            # This ensures we wait for 'quiet time' before emitting.
            timer.set(time.time() + self.buffer_seconds)
        else:
            PipelineMetrics.dedup_dropped.inc()
            yield beam.pvalue.TaggedOutput(
                "dropped",
                {
                    "event": event,
                    "reason": "duplicate_or_older_event",
                    "existing_ts": existing_ts,
                    "incoming_ts": new_ts,
                }
            )
    

    @on_timer(emit_timer)
    def emit_final(
        self, 
        event_state=beam.DoFn.StateParam(latest_event_state),
        ts_state=beam.DoFn.StateParam(latest_ts_state)
    ):
        final_event = event_state.read()
        if final_event:
            PipelineMetrics.dedup_emitted.inc()
            yield final_event
        
        # Clear state to prevent memory leaks and allow future updates for this ID
        event_state.clear()
        ts_state.clear()

class DeduplicateLatest(beam.PTransform):
    """
    Industry-Standard Deduplication:
    1. Forces Global Window to ensure IDs find each other.
    2. Uses Stateful Snooze timer to emit only the latest event.
    """
    def __init__(self, buffer_seconds: int = 30, max_state_age_sec: int = 1800):
        self.buffer_seconds = buffer_seconds
        self.max_state_age_sec = max_state_age_sec

    def expand(self, pcoll):
        return (
            pcoll
            # 1. Force GlobalWindow so all event_ids hit the same state cell
            | "ForceGlobalWindow" >> beam.WindowInto(beam.window.GlobalWindows())
            | "KeyByEventId" >> beam.Map(lambda e: (e.get("event_id", "unknown"), e))
            | "StatefulBuffer" >> beam.ParDo(
                _BufferedDedupDoFn(
                    buffer_seconds=self.buffer_seconds,
                    max_state_age_sec=self.max_state_age_sec,
                )
            ).with_outputs("dropped", main="main")
        )