import apache_beam as beam
from pipeline.observability.metrics import PipelineMetrics

class TrackInput(beam.DoFn):
    def process(self, element):
        PipelineMetrics.events_in.inc()
        yield element

class TrackOutput(beam.DoFn):
    def process(self, element):
        PipelineMetrics.events_out.inc()
        yield element
