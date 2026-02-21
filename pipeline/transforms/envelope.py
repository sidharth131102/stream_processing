import logging
import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp
from datetime import datetime, timezone

class Envelope(beam.DoFn):
    def process(self, event, ts=beam.DoFn.TimestampParam):
        # Beam event-time (already a Beam Timestamp)
        event["beam_event_time"] = ts

        # Processing time as Beam Timestamp
        event["beam_processing_time"] = Timestamp.from_utc_datetime(
            datetime.now(timezone.utc)
        )
        logging.info(f"Enveloped event_id: {event.get('event_id', 'unknown_id')} with beam_event_time: {event['beam_event_time'].to_utc_datetime().isoformat()} and beam_processing_time: {event['beam_processing_time'].to_utc_datetime().isoformat()}")

        yield event
