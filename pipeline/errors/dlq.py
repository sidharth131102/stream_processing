import apache_beam as beam
from pipeline.observability.metrics import PipelineMetrics
import json
from datetime import datetime
from apache_beam.utils.timestamp import Timestamp

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
            # 1. Handle Apache Beam Timestamps
            if isinstance(obj, Timestamp):
                return obj.to_rfc3339()
            
            # 2. Handle standard Python datetimes
            if isinstance(obj, datetime):
                return obj.isoformat()

            # 3. Handle bytes (your existing logic)
            if isinstance(obj, bytes):
                return obj.decode("utf-8", errors="replace")
            
            # 4. Fallback: Convert to string instead of raising TypeError
            # This is the "Industry Standard" way to prevent DLQ crashes
            return str(obj)

        return json.dumps(event, default=json_serializable).encode("utf-8")

    @staticmethod
    def _count(event):
        PipelineMetrics.dlq_events.inc()
        stage = event.get("stage", "unknown")
        PipelineMetrics.stage_error(stage).inc()
        return event