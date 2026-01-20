import apache_beam as beam
from apache_beam.metrics import Metrics

from pipeline.schema.schema_utils import (
    extract_runtime_schema,
    diff_schema,
    has_schema_changed,
)


class SchemaGuard(beam.DoFn):
    """
    Detects schema evolution for full envelope + payload.
    Non-blocking by default.
    """

    def __init__(self, cfg: dict, expected_schema: dict):
        self.cfg = cfg
        self.expected_schema = expected_schema

        # Metrics
        self.evolution_detected = Metrics.counter(
            "schema", "evolution_detected"
        )

    def process(self, event: dict):
        # Safety toggle
        if not self.cfg.get("enabled", False):
            yield event
            return

        # Extract observed schema
        observed_schema = extract_runtime_schema(event)

        # Compare schemas
        diff = diff_schema(self.expected_schema, observed_schema)

        if not has_schema_changed(diff):
            yield event
            return

        # 🚨 Schema evolution detected
        self.evolution_detected.inc()

        # Always allow event to continue (detection-only mode)
        yield event

        # Emit schema evolution record
        yield beam.pvalue.TaggedOutput(
            "schema_dlq",
            {
                "event_id": event.get("event_id"),
                "schema_version": self.cfg.get("schema_version"),
                "diff": diff,
                "observed_schema": observed_schema,
            }
        )
