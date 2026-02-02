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

          -- Preserve original event timestamp contract
          event_ts_raw AS event_ts,

          -- Payload rehydration
          raw_payload AS payload,

          -- Pub/Sub metadata (may be NULL for older data)
          pubsub_metadata AS _pubsub

        FROM `{self.table}`
        WHERE event_ts >= TIMESTAMP(@start_ts)
          AND event_ts <  TIMESTAMP(@end_ts)
        """

        return (
            p
            | "ReadRawBQ"
            >> beam.io.ReadFromBigQuery(
                query=query,
                use_standard_sql=True,
                query_parameters=[
                    {
                        "name": "start_ts",
                        "parameterType": {"type": "TIMESTAMP"},
                        "parameterValue": {"value": self.start_ts},
                    },
                    {
                        "name": "end_ts",
                        "parameterType": {"type": "TIMESTAMP"},
                        "parameterValue": {"value": self.end_ts},
                    },
                ],
                project=self.project,
                location=self.location,
            )
        )
