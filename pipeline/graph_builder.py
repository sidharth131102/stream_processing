import apache_beam as beam

from pipeline.io_connectorss.gcs_archive_writer import WriteRawArchive
from pipeline.io_connectorss.bq_writer import write_bq
from pipeline.errors.dlq import WriteDLQ
import logging
from pipeline.transforms.batch_dedup import BatchDeduplicateLatest
from pipeline.transforms.preprocess import PreProcess
from pipeline.transforms.envelope import Envelope
from pipeline.transforms.parser import ParseEvent
from pipeline.transforms.transformation_engine import TransformationEngine
from pipeline.transforms.dedup import DeduplicateLatest   # ✅ NEW
from pipeline.windowing.event_time import AssignEventTime
from pipeline.windowing.windows import ApplyWindows
from pipeline.validators.soft_validator import SoftValidate
from pipeline.observability.trackers import TrackInput, TrackOutput
from pipeline.observability.lag import TrackEventLag
from pipeline.schema.schema_guard import SchemaGuard
from pipeline.io_connectorss.source_factory import read_source

def build_pipeline(p, cfg, subscription):
    logging.info(f"PIPELINE MODE = {cfg.get('job_mode')}")
    streaming = cfg["streaming_tuning"]

    # ==================================================
    # 1️⃣ Read + Preprocess
    # ==================================================
    preprocess = (
        read_source(p, cfg, subscription)
        | "TrackInput" >> beam.ParDo(TrackInput())
        | "PreProcess"
        >> beam.ParDo(PreProcess()).with_outputs("dlq", main="main")
    )

    main = preprocess.main
    preprocess_dlq = preprocess.dlq
    if (
        cfg.get("job_mode") == "streaming"
        and cfg.get("archive", {}).get("enabled", False)
    ):
        main | "WriteRawArchive" >> WriteRawArchive(cfg["archive"])
    # ==================================================
    # 2️⃣ Envelope + Parse + Assign Event Time
    # ==================================================
    parsed = (
        main
        | "Envelope" >> beam.ParDo(Envelope())
        | "ParseEvent" >> beam.ParDo(ParseEvent(cfg["source"]))
    )

    schema_checked = (
        parsed
        | "SchemaGuard"
        >> beam.ParDo(
            SchemaGuard(
                cfg["schema_management"],
                cfg["expected_schema"]
            )
        ).with_outputs("schema_dlq", main="main")
    )

    main = schema_checked.main
    schema_dlq = schema_checked.schema_dlq

    # Now assign event time
    parsed_with_time = (
        main
        | "AssignEventTime"
        >> beam.ParDo(AssignEventTime()).with_outputs("dlq", main="main")
    )

    main = parsed_with_time.main
    event_time_dlq = parsed_with_time.dlq
    
    if cfg.get("job_mode") == "streaming":
        main = main | "TrackEventLag" >> beam.ParDo(TrackEventLag())

    # ==================================================
    # 3️⃣ DEDUPLICATION (GLOBAL, STATEFUL, FIRST)
    # ==================================================
    dedup_cfg = streaming.get("dedup", {})

    if cfg.get("job_mode") == "streaming":
        if dedup_cfg.get("mode", "single") != "none":
            main = (
                main
                | "DeduplicateLatest"
                >> DeduplicateLatest(
                    buffer_seconds=dedup_cfg["buffer_seconds"],
                    max_state_age_sec=dedup_cfg.get("max_state_age_sec", 1800),
                )
            )

    elif cfg.get("job_mode") == "backfill":
        # Batch-safe dedup (NO STATE, NO TIMERS)
        main = main | "BatchDeduplicateLatest" >> BatchDeduplicateLatest()


    # ==================================================
    # 4️⃣ WINDOWING (AFTER DEDUP)
    # ==================================================
    if (
        cfg.get("job_mode") == "streaming"
        and streaming.get("windowing", {}).get("enabled", True)
    ):
        main = main | "ApplyWindows" >> ApplyWindows(streaming["windowing"])

    # ==================================================
    # 5️⃣ Transform
    # ==================================================
    transformed = (
        main
        | "Transform"
        >> beam.ParDo(
            TransformationEngine(cfg["transformations"])
        ).with_outputs("dlq", main="main")
    )

    main = transformed.main
    transform_dlq = transformed.dlq

    # ==================================================
    # 6️⃣ Validation
    # ==================================================
    validated = (
        main
        | "Validate"
        >> beam.ParDo(
            SoftValidate(cfg["validation"])
        ).with_outputs("dlq", main="main")
    )

    main = validated.main
    validation_dlq = validated.dlq

    # ==================================================
    # 7️⃣ OPTIONAL BATCHING
    # ==================================================
    batching_cfg = streaming.get("batching", {}).get("bigquery", {})
    if batching_cfg.get("enabled", False):
        main = (
            main
            | "BatchForBQ"
            >> beam.BatchElements(
                min_batch_size=batching_cfg["min_batch_size"],
                max_batch_size=batching_cfg["max_batch_size"],
            )
        )

    # ==================================================
    # 8️⃣ Sink
    # ==================================================
    main = main | "TrackOutput" >> beam.ParDo(TrackOutput())
    write_bq(main, cfg)

    # Resolve DLQ topic (streaming vs backfill)
    dlq_topic = (
        cfg["destination"]["dlq_topic"]
        if cfg.get("job_mode") == "streaming"
        else cfg.get("backfill_yaml", {})
            .get("dlq", {})
            .get("topic", cfg["destination"]["dlq_topic"])
    )


    # ==================================================
    # 9️⃣ DLQ
    # ==================================================
    (
        (
            preprocess_dlq
            | "TagPreprocessDLQ"
            >> beam.Map(lambda e: {"stage": "preprocess", **e}),
            event_time_dlq
            | "TagEventTimeDLQ"
            >> beam.Map(lambda e: {"stage": "event_time", **e}),
            transform_dlq
            | "TagTransformDLQ"
            >> beam.Map(lambda e: {"stage": "transform", **e}),
            validation_dlq
            | "TagValidationDLQ"
            >> beam.Map(lambda e: {"stage": "validation", **e}),
            schema_dlq
            | "TagSchemaDLQ"
            >> beam.Map(lambda e: {"stage": "schema", **e}),
        )
        | "FlattenDLQ" >> beam.Flatten()
        | "WriteDLQ" >> WriteDLQ(dlq_topic)
    )
