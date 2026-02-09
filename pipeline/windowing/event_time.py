import apache_beam as beam
from datetime import datetime, timezone
from apache_beam.utils.timestamp import Timestamp

class AssignEventTime(beam.DoFn):
    def process(self, event):
        ts = event.get("event_ts")

        # 1️⃣ Type validation
        if not isinstance(ts, str):
            event["error"] = "event_ts is not a string"
            event["_dlq_reason"] = "event_time_invalid_type"
            yield beam.pvalue.TaggedOutput("dlq", event)
            return  # 🔥 CRITICAL: stops retries

        # 2️⃣ Parse validation
        try:
            ts = ts.replace("Z", "+00:00")
            dt = datetime.fromisoformat(ts).astimezone(timezone.utc)
        except Exception:
            event["error"] = "Invalid ISO-8601 event_ts"
            event["_dlq_reason"] = "event_time_parse_failed"
            yield beam.pvalue.TaggedOutput("dlq", event)
            return  # 🔥 CRITICAL: stops retries

        # 3️⃣ Happy path (ACK happens)
        event["event_timestamp"] = dt.timestamp()

        yield beam.window.TimestampedValue(
            event,
            Timestamp.from_utc_datetime(dt)
        )
