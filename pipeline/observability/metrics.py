from apache_beam.metrics import Metrics

class PipelineMetrics:
    """
    Centralized metrics registry for the streaming accelerator.
    """

    # Throughput
    events_in = Metrics.counter("pipeline", "events_in")
    events_out = Metrics.counter("pipeline", "events_out")

    # Errors
    parse_errors = Metrics.counter("errors", "parse_errors")
    validation_errors = Metrics.counter("errors", "validation_errors")
    transform_errors = Metrics.counter("errors", "transform_errors")

    # DLQ
    dlq_events = Metrics.counter("dlq", "events_total")

    # Dedup
    dedup_seen = Metrics.counter("dedup", "events_seen")
    dedup_emitted = Metrics.counter("dedup", "events_emitted")
    dedup_dropped = Metrics.counter("dedup", "events_dropped")

    @staticmethod
    def stage_error(stage: str):
        return Metrics.counter("stage_errors", stage)
