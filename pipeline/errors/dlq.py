import apache_beam as beam
from pipeline.observability.metrics import PipelineMetrics
import json


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
        """Convert event dict to JSON bytes, handling bytes values."""

        def json_serializable(obj):
            if isinstance(obj, bytes):
                return obj.decode("utf-8", errors="replace")
            raise TypeError(
                f"Object of type {type(obj).__name__} is not JSON serializable"
            )

        return json.dumps(event, default=json_serializable).encode("utf-8")

    @staticmethod
    def _count(event):
        PipelineMetrics.dlq_events.inc()
        stage = event.get("stage", "unknown")
        PipelineMetrics.stage_error(stage).inc()
        return event