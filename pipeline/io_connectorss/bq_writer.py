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


def _json_type_to_bq(json_type):
    raw = str(json_type or "").strip().lower()
    parts = [p.strip() for p in raw.split("|") if p.strip()]
    non_null = [p for p in parts if p != "null"]
    t = non_null[0] if non_null else (parts[0] if parts else "")
    if t == "integer":
        return "INT64"
    if t == "float":
        return "FLOAT64"
    if t == "boolean":
        return "BOOL"
    if t == "date":
        return "DATE"
    if t == "time":
        return "TIME"
    if t == "datetime":
        return "DATETIME"
    if t == "timestamp":
        return "TIMESTAMP"
    if t == "string":
        return "STRING"
    return "STRING"


def _normalize_bq_type(bq_type):
    if not bq_type:
        return "STRING"
    t = str(bq_type).strip().upper()
    if t == "INTEGER":
        return "INT64"
    if t == "FLOAT":
        return "FLOAT64"
    if t == "BOOLEAN":
        return "BOOL"
    return t


def _collect_destination_fields(dest_cfg, transform_cfg, expected_schema=None):
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

    # Map source path -> output column for flattened payload fields.
    source_to_target = {}
    for target, mapping in field_mapping.items():
        if isinstance(mapping, str):
            source_to_target[mapping] = target

    # Prefer schema-guard contract types when present.
    for source_field, source_type in (expected_schema or {}).items():
        target_field = source_field

        if source_field.startswith("payload."):
            target_field = source_to_target.get(source_field)
            if not target_field:
                continue

        fields[target_field] = _json_type_to_bq(source_type)

    for target, mapping in field_mapping.items():
        if isinstance(mapping, dict):
            fields[target] = _normalize_bq_type(mapping.get("type", "STRING"))
        else:
            fields.setdefault(target, "STRING")

    for rule in transform_cfg.get("business_rules", []):
        fields[rule["output_field"]] = "BOOLEAN"

    for enrich in transform_cfg.get("enrichment", []):
        etype = enrich.get("type")

        if etype == "derived_fields":
            for item in enrich.get("fields", []):
                out = item.get("output_field")
                if out:
                    fields[out] = _normalize_bq_type(item.get("type", "STRING"))
            continue

        out = enrich.get("output_field")
        if out:
            fields[out] = _normalize_bq_type(enrich.get("output_type", "STRING"))

    for concat in transform_cfg.get("concatenations", []):
        fields[concat["output_field"]] = _normalize_bq_type(
            concat.get("type", "STRING")
        )

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
    expected_schema = cfg.get("expected_schema", {})

    table = _resolve_table(cfg)

    schema = {
        "fields": _collect_destination_fields(dest, transforms, expected_schema)
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
