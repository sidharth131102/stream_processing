import apache_beam as beam
import hashlib
import json


def _resolve_table(cfg):
    dest = cfg["destination"]
    project = dest["target"]["project"]

    if cfg.get("job_mode") == "backfill":
        if "temp_table" not in dest:
            raise ValueError(
                "Backfill mode requires destination.temp_table configuration"
            )

        run_id = cfg["backfill"].get("run_id")

        if not run_id:
            start = cfg["backfill"]["start_ts"].replace(":", "").replace("-", "")
            end = cfg["backfill"]["end_ts"].replace(":", "").replace("-", "")
            run_id = f"{start}_{end}"

        dataset = dest["temp_table"]["dataset"]
        prefix = dest["temp_table"]["table_prefix"]

        return f"{project}:{dataset}.{prefix}_{run_id}"


    # Streaming (or default)
    return (
        f"{project}:"
        f"{dest['target']['dataset']}."
        f"{dest['target']['table']}"
    )


def _collect_destination_fields(dest_cfg, source_cfg, transform_cfg):
    fields = {
        "event_id": "STRING",
        "event_type": "STRING",
        "event_source": "STRING",
        "event_ts": "STRING",
        "event_timestamp": "FLOAT64",
        "row_id": "STRING",
    }

    for name, bq_type in dest_cfg.get("envelope_fields", {}).items():
        fields[name] = bq_type

    field_mapping = transform_cfg.get("field_mapping", {})
    for target, mapping in field_mapping.items():
        if isinstance(mapping, dict):
            fields[target] = mapping.get("type", "STRING")
        else:
            fields[target] = "STRING"

    for rule in transform_cfg.get("business_rules", []):
        fields[rule["output_field"]] = "BOOLEAN"

    for enrich in transform_cfg.get("enrichment", []):
        fields[enrich["output_field"]] = "STRING"

    for cfg in transform_cfg.get("entity_extraction", []):
        for ent in cfg.get("entities", []):
            fields[ent["name"]] = "STRING"

    return [{"name": k, "type": v, "mode": "NULLABLE"} for k, v in fields.items()]


def _with_row_id(row):
    stable = {
        "event_id": row.get("event_id"),
        "event_ts": row.get("event_ts"),
    }
    row["row_id"] = hashlib.md5(
        json.dumps(stable, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return row


def _normalize_rows(element):
    if isinstance(element, list):
        return element
    return [element]


def write_bq(pcoll, cfg):
    dest = cfg["destination"]
    transforms = cfg["transformations"]

    table = _resolve_table(cfg)

    schema = {
        "fields": _collect_destination_fields(dest, cfg["source"], transforms)
    }

    # ----------------------------
    # Mode-aware write config
    # ----------------------------
    write_kwargs = {
        "table": table,
        "schema": schema,
        "create_disposition": beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        "method": beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
    }

    if cfg.get("job_mode") == "backfill":
        write_kwargs.update({
            "write_disposition": beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        })
    else:
        write_kwargs.update({
            "write_disposition": beam.io.BigQueryDisposition.WRITE_APPEND,
            "triggering_frequency": 60,  # streaming only
        })

    (
        pcoll
        | "NormalizeRows" >> beam.FlatMap(_normalize_rows)
        | "AddRowId" >> beam.Map(_with_row_id)
        | "WriteBQ" >> beam.io.WriteToBigQuery(**write_kwargs)
    )
