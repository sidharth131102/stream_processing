import apache_beam as beam
from pipeline.observability.metrics import PipelineMetrics
import json
from datetime import datetime
from apache_beam.utils.timestamp import Timestamp
import logging   # ✅ ADD

class WriteDLQ(beam.PTransform):
    def __init__(self, dlq_topic: str):
        if not dlq_topic:
            raise ValueError("DLQ topic must be provided")
        self.dlq_topic = dlq_topic

    def expand(self, pcoll):
        return (
            pcoll
            | "CountDLQ" >> beam.Map(self._count)
            | "DLQToJson" >> beam.Map(self._to_json_bytes)
            | "WriteDLQToPubSub" >> beam.io.WriteToPubSub(self.dlq_topic)
        )

    @staticmethod
    def _to_json_bytes(event):
        """Convert event dict to JSON bytes, handling timestamps and bytes."""

        def json_serializable(obj):
            if isinstance(obj, Timestamp):
                try:
                    return obj.to_rfc3339()
                except Exception:
                    return "invalid-beam-timestamp"
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, bytes):
                return obj.decode("utf-8", errors="replace")
            return str(obj)

        return json.dumps(event, default=json_serializable).encode("utf-8")

    @staticmethod
    def _count(event):
        # ✅ EXISTING METRICS (UNCHANGED)
        PipelineMetrics.dlq_events.inc()
        stage = event.get("stage", "unknown")
        PipelineMetrics.stage_error(stage).inc()

        # ✅ ADD: STRUCTURED LOGGING FOR DLQ EVENTS
        logging.error(json.dumps({
            "severity": "ERROR",
            "type": "DLQ_EVENT",
            "stage": stage,
            "message": event.get("error"),
        }))

        return event
