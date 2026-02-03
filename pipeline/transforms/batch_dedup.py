import apache_beam as beam
from datetime import datetime

# ✅ ADD
from pipeline.observability.metrics import PipelineMetrics


def _event_ts_as_float(event):
    ts = event.get("event_ts")

    if not ts or not isinstance(ts, str):
        return float("-inf")

    try:
        return datetime.fromisoformat(
            ts.replace("Z", "+00:00")
        ).timestamp()
    except Exception:
        # Treat invalid timestamps as oldest
        return float("-inf")




def _pick_latest_and_track(kv):
    """
    kv: (event_id, Iterable[event])
    """
    events = list(kv[1])

    # ✅ ADD: seen = number of records for this key
    for _ in events:
        PipelineMetrics.dedup_seen.inc()

    latest = max(events, key=_event_ts_as_float)

    # ✅ ADD: emitted = 1
    PipelineMetrics.dedup_emitted.inc()

    # ✅ ADD: dropped = rest
    dropped_count = max(len(events) - 1, 0)
    for _ in range(dropped_count):
        PipelineMetrics.dedup_dropped.inc()

    return latest


class BatchDeduplicateLatest(beam.PTransform):
    """
    Deterministic batch deduplication:
    - Key by event_id
    - Keep the latest event by event_ts
    """

    def expand(self, pcoll):
        return (
            pcoll
            | "KeyByEventId" >> beam.Map(lambda e: (e["event_id"], e))
            | "GroupByEventId" >> beam.GroupByKey()
            | "PickLatestByEventTs" >> beam.Map(_pick_latest_and_track)
        )
