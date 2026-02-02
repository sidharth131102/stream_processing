import apache_beam as beam
from datetime import datetime, timezone
import logging
class AssignEventTime(beam.DoFn):
    def process(self, event):
        try:
            ts = event.get("event_ts")

            if not isinstance(ts, str):
                raise ValueError("event_ts must be ISO-8601 string")

            ts = ts.replace("Z", "+00:00")
            dt = datetime.fromisoformat(ts).astimezone(timezone.utc)

            # Keep for downstream usage (BQ, rules, etc.)
            event["event_timestamp"] = dt.timestamp()
            logging.error("🔥 NEW AssignEventTime CODE ACTIVE")

            # 🔥 THIS IS THE KEY FIX
            yield beam.window.TimestampedValue(event, dt.timestamp())

        except Exception as e:
            yield beam.pvalue.TaggedOutput(
                "dlq",
                {
                    "event": event,
                    "error": str(e),
                },
            )
