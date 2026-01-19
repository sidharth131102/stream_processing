import apache_beam as beam

class RouteEvent(beam.DoFn):
    def process(self, event):
        yield beam.pvalue.TaggedOutput(event["event_type"], event)
