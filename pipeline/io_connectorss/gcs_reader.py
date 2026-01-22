import apache_beam as beam
import json


class ReadFromGCSArchive(beam.PTransform):
    """
    Reads archived raw JSON events from GCS for backfill.
    Emits dict events.
    """

    def __init__(self, path_pattern: str):
        self.path_pattern = path_pattern

    def expand(self, p):
        return (
            p
            | "ReadRawJsonFiles"
            >> beam.io.ReadFromText(self.path_pattern)
            | "ParseJson"
            >> beam.Map(json.loads)
        )
