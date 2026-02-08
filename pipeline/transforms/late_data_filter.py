import apache_beam as beam
from datetime import timedelta

class LateDataFilter(beam.DoFn):
    def __init__(self, allowed_lateness_sec: int):
        self.allowed_lateness_sec = allowed_lateness_sec

    def process(self, event):
        event_time = event["beam_event_time"]
        processing_time = event["beam_processing_time"]

        lateness_sec = (
            processing_time.micros - event_time.micros
        ) / 1_000_000

        if lateness_sec > self.allowed_lateness_sec:
            yield beam.pvalue.TaggedOutput(
                "dlq",
                {
                    "event": event,
                    "error": f"Late event dropped (lateness={lateness_sec}s)",
                    "lateness_seconds": lateness_sec,
                },
            )
        else:
            yield event
