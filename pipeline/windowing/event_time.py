import logging
import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp
from pipeline.utils.time_parser import parse_event_ts_to_utc_datetime

class AssignEventTime(beam.DoFn):
    def process(self, event):
        ts = event.get("event_ts")

        # Parse validation across supported timestamp formats.
        try:
            dt = parse_event_ts_to_utc_datetime(ts)
        except Exception:
            event["error"] = "Unsupported event_ts format"
            event["_dlq_reason"] = "event_time_parse_failed"
            yield beam.pvalue.TaggedOutput("dlq", event)
            return  # 🔥 CRITICAL: stops retries

        # Happy path (ACK happens)
        event["event_timestamp"] = dt.timestamp()

        yield beam.window.TimestampedValue(
            event,
            Timestamp.from_utc_datetime(dt)
        )
        logging.info(f"Assigned event time for event_id: {event.get('event_id', 'unknown_id')} with timestamp: {dt.isoformat()}")
