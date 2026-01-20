import apache_beam as beam
from apache_beam.coders import PickleCoder
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
    TimeDomain,
)
from apache_beam.utils.timestamp import Timestamp, Duration
from datetime import datetime

from pipeline.observability.metrics import PipelineMetrics


# ----------------------------------
# Helpers
# ----------------------------------
def _parse_event_ts(event):
    try:
        ts = event.get("event_ts")
        if not ts:
            return 0.0
        return datetime.fromisoformat(
            ts.replace("Z", "+00:00")
        ).timestamp()
    except Exception:
        return 0.0


# ----------------------------------
# Stateful DoFn
# ----------------------------------
class _BufferedDedupDoFn(beam.DoFn):
    """
    Global latest-event deduplication per event_id.
    """

    latest_state = ReadModifyWriteStateSpec("latest", PickleCoder())
    emit_timer = TimerSpec("emit", TimeDomain.PROCESSING_TIME)

    def __init__(self, buffer_seconds: int, max_state_age_sec: int):
        self.buffer_seconds = buffer_seconds
        self.max_state_age_sec = max_state_age_sec

    def process(
        self,
        element,
        latest=beam.DoFn.StateParam(latest_state),
        timer=beam.DoFn.TimerParam(emit_timer),
    ):
        key, event = element

        # ---- metrics: seen ----
        PipelineMetrics.dedup_seen.inc()

        new_ts = _parse_event_ts(event)
        if new_ts <= 0:
            PipelineMetrics.dedup_dropped.inc()
            return

        existing = latest.read()
        existing_ts = _parse_event_ts(existing) if existing else -1

        # Hard TTL guard (state safety)
        if existing and (new_ts - existing_ts) > self.max_state_age_sec:
            latest.clear()
            existing = None

        # Keep latest event
        if existing is None or new_ts > existing_ts:
            latest.write(event)
        else:
            PipelineMetrics.dedup_dropped.inc()
            return

        # Schedule buffered emission in event-time
        timer.set(
            Timestamp(new_ts) + Duration(seconds=self.buffer_seconds)
        )

    @on_timer(emit_timer)
    def emit_final(self, latest=beam.DoFn.StateParam(latest_state)):
        final_event = latest.read()
        if final_event:
            PipelineMetrics.dedup_emitted.inc()
            yield final_event
        else:
            PipelineMetrics.dedup_dropped.inc()

        latest.clear()


# ----------------------------------
# PTransform Wrapper
# ----------------------------------
class DeduplicateLatest(beam.PTransform):
    """
    Deduplicate globally by event_id.
    Emits the latest event after buffer_seconds.
    """

    def __init__(self, buffer_seconds: int = 30, max_state_age_sec: int = 1800):
        self.buffer_seconds = buffer_seconds
        self.max_state_age_sec = max_state_age_sec

    def expand(self, pcoll):
        return (
            pcoll
            | "KeyByEventId"
            >> beam.Map(lambda e: (e.get("event_id", "unknown"), e))
            | "BufferedDedup"
            >> beam.ParDo(
                _BufferedDedupDoFn(
                    buffer_seconds=self.buffer_seconds,
                    max_state_age_sec=self.max_state_age_sec,
                )
            )
        )
