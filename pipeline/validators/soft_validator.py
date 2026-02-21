import apache_beam as beam
from datetime import datetime

# ✅ ADD
import logging
import json
from pipeline.observability.metrics import PipelineMetrics


class SoftValidate(beam.DoFn):
    def __init__(self, validation_cfg):
        self.cfg = validation_cfg

    def process(self, event):
        errors = []

        # 1️⃣ Required envelope fields
        for field in (self.cfg.get("required_fields") or []):
            if field not in event or event[field] in (None, ""):
                errors.append(f"Missing required field: {field}")

        # 2️⃣ Required payload fields (FIXED: Check root level after FieldMapper flattening)
        for field in (self.cfg.get("payload_required_fields") or []):
            if field not in event or event[field] in (None, ""):
                errors.append(f"Missing required payload field: {field}")

        # 3️⃣ ISO-8601 timestamp validation for event_ts
        event_ts = event.get("event_ts")
        if event_ts is not None:
            if not isinstance(event_ts, str):
                errors.append("event_ts must be a STRING (ISO-8601)")
            else:
                try:
                    ts = event_ts.replace("Z", "+00:00")
                    datetime.fromisoformat(ts)
                except Exception:
                    errors.append("event_ts is not a valid ISO-8601 timestamp")

        # 4️⃣ Length checks
        for field, rules in (self.cfg.get("length_checks") or {}).items():
            value = event.get(field)

            if value is None:
                continue

            if not isinstance(value, str):
                value = str(value)

            if "max" in rules and len(value) > rules["max"]:
                errors.append(f"{field} exceeds max length")

            if "min" in rules and len(value) < rules["min"]:
                errors.append(f"{field} below min length")

            if "equal" in rules and len(value) != rules["equal"]:
                errors.append(f"{field} must be exactly {rules['equal']} characters")


        # 5️⃣ Type checks (soft, non-blocking)
        for field, expected in (self.cfg.get("type_checks") or {}).items():
            value = event.get(field)
            if value is None:
                continue

            if expected == "STRING" and not isinstance(value, str):
                errors.append(f"{field} not STRING")
            elif expected == "INTEGER" and not isinstance(value, int):
                errors.append(f"{field} not INTEGER")
            elif expected == "BOOLEAN" and not isinstance(value, bool):
                errors.append(f"{field} not BOOLEAN")

        # 6️⃣ Route output
        if errors:
            # ✅ ADD: METRICS
            PipelineMetrics.validation_errors.inc()
            PipelineMetrics.stage_error("validation").inc()

            # ✅ ADD: STRUCTURED LOGGING
            logging.warning(json.dumps({
                "severity": "WARNING",
                "stage": "validation",
                "errors": errors,
                "event_id": event.get("event_id"),
            }))

            yield beam.pvalue.TaggedOutput(
                "dlq",
                {
                    "stage": "validation",
                    "event": event,
                    "validation_errors": errors,
                },
            )
        else:
            yield event
