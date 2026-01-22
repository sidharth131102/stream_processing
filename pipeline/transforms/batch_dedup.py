import apache_beam as beam
from datetime import datetime


def _event_ts_as_float(event: dict) -> float:
    ts = event.get("event_ts")
    if not ts:
        return 0.0
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()


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
            | "PickLatestByEventTs"
            >> beam.Map(lambda kv: max(kv[1], key=_event_ts_as_float))
        )
