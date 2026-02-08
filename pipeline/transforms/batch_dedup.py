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
    event_id, events = kv
    events = list(events)

    for _ in events:
        PipelineMetrics.dedup_seen.inc()

    latest = max(events, key=_event_ts_as_float)

    PipelineMetrics.dedup_emitted.inc()

    for e in events:
        if e is not latest:
            PipelineMetrics.dedup_dropped.inc()
            yield beam.pvalue.TaggedOutput(
                "dropped",
                {
                    "event": e,
                    "reason": "batch_duplicate",
                    "kept_event_ts": latest.get("event_ts"),
                }
            )

    yield latest



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
            | "PickLatestByEventTs" >> beam.Map(_pick_latest_and_track).with_outputs("dropped", main="main")
        )
