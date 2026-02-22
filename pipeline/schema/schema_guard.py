import apache_beam as beam
from apache_beam.metrics import Metrics

from pipeline.schema.schema_utils import (
    extract_runtime_schema,
    diff_schema,
    has_schema_changed,
)

# ✅ ADD
import logging
import json
from pipeline.observability.metrics import PipelineMetrics


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

        # Schema evolution detected
        self.evolution_detected.inc()

        # Stage error metric
        PipelineMetrics.stage_error("schema").inc()

        policy = self.cfg.get("evolution_policy", {})
        on_new = str(policy.get("on_new_field", "ALLOW")).upper()
        on_missing = str(policy.get("on_missing_field", "WARN")).upper()
        on_type = str(policy.get("on_type_change", "WARN")).upper()

        fail_reasons = []
        if diff.get("new_fields") and on_new == "FAIL":
            fail_reasons.append("new_fields")
        if diff.get("missing_fields") and on_missing == "FAIL":
            fail_reasons.append("missing_fields")
        if diff.get("type_changes") and on_type == "FAIL":
            fail_reasons.append("type_changes")

        should_fail = bool(fail_reasons)

        # Structured logging
        logging.warning(json.dumps({
            "severity": "WARNING",
            "stage": "schema",
            "type": "SCHEMA_EVOLUTION",
            "event_id": event.get("event_id"),
            "schema_version": self.cfg.get("schema_version"),
            "fail_reasons": fail_reasons,
            "diff": diff,
        }))

        # Route to schema DLQ on policy violation and stop main output.
        if should_fail:
            if self.cfg.get("dlq_on_violation", True):
                yield beam.pvalue.TaggedOutput(
                    "schema_dlq",
                    {
                        "event_id": event.get("event_id"),
                        "schema_version": self.cfg.get("schema_version"),
                        "error": f"Schema policy violation: {','.join(fail_reasons)}",
                        "diff": diff,
                        "observed_schema": observed_schema,
                    }
                )
            return

        # Non-failing evolution is allowed to continue; still emit trace event.
        yield event
        yield beam.pvalue.TaggedOutput(
            "schema_dlq",
            {
                "event_id": event.get("event_id"),
                "schema_version": self.cfg.get("schema_version"),
                "error": None,
                "diff": diff,
                "observed_schema": observed_schema,
            }
        )
