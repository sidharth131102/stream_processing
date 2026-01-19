import apache_beam as beam
import time
from apache_beam.metrics import Metrics

class TrackEventLag(beam.DoFn):
    lag_ms = Metrics.distribution("latency", "event_lag_ms")

    def process(self, element, ts=beam.DoFn.TimestampParam):
        now_ms = int(time.time() * 1000)
        event_ms = int(ts.micros / 1000)
        self.lag_ms.update(now_ms - event_ms)
        yield element
