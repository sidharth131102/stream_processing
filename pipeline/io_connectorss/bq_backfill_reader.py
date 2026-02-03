import apache_beam as beam


class ReadFromRawBQ(beam.PTransform):
    """
    Bounded BigQuery source for windowed backfill.
    Reads immutable raw events and rehydrates the streaming contract.
    """

    def __init__(self, cfg: dict):
        self.table = cfg["table"]
        self.start_ts = cfg["start_ts"]
        self.end_ts = cfg["end_ts"]
        self.project = cfg.get("project")
        self.location = cfg.get("location")

    def expand(self, p):
        query = f"""
        SELECT
          event_id,
          event_type,
          event_source,
          CAST(event_ts_raw AS STRING) AS event_ts,
          raw_payload AS payload,
          pubsub_metadata AS _pubsub 
        FROM `{self.table}`
        WHERE TIMESTAMP(event_ts_raw) >= TIMESTAMP('{self.start_ts}')
          AND TIMESTAMP(event_ts_raw) <  TIMESTAMP('{self.end_ts}')
        """

        return (
            p
            | "ReadRawBQ"
            >> beam.io.ReadFromBigQuery(
                query=query,
                use_standard_sql=True,
                project=self.project,
            )
        )
