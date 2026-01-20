import yaml
import json
from google.cloud import storage


def load_all_configs(bucket: str):
    """
    Loads ALL runtime configs from GCS:
      - source_mapping.yaml
      - destination_mapping.yaml
      - transformation.yaml
      - validation.yaml
      - pipeline.yaml

    Returns merged config dict.
    """

    client = storage.Client()
    gcs_bucket = client.bucket(bucket)

    def _load_yaml(name: str):
        blob = gcs_bucket.blob(name)
        if not blob.exists():
            raise FileNotFoundError(
                f"Required config '{name}' not found in gs://{bucket}"
            )
        return yaml.safe_load(blob.download_as_text())
    
    def _load_json(name: str):
        blob = gcs_bucket.blob(name)
        if not blob.exists():
            raise FileNotFoundError(f"Missing schema file: {name}")
        return json.loads(blob.download_as_text())


    cfg = {
        "source": _load_yaml("source_mapping.yaml"),
        "destination": _load_yaml("destination_mapping.yaml"),
        "transformations": _load_yaml("transformation.yaml"),
        "validation": _load_yaml("validation.yaml"),
    }

    for k, v in cfg.items():
        if not v:
            raise ValueError(f"Config '{k}' loaded as empty")

    pipeline_cfg = _load_yaml("pipeline.yaml")
    schema_cfg = pipeline_cfg.get("schema_management", {})
    if schema_cfg.get("enabled", False):
        schema_name = schema_cfg["schema_name"]
        cfg["expected_schema"] = _load_json(f"schemas/{schema_name}.json")
        cfg["schema_management"] = schema_cfg


    streaming_tuning = pipeline_cfg.get("streaming_tuning")
    if not streaming_tuning:
        raise KeyError("pipeline.yaml missing 'streaming_tuning'")

    cfg["streaming_tuning"] = streaming_tuning
    required_blocks = ["dedup", "sharding", "windowing", "batching"]
    for block in required_blocks:
        if block not in streaming_tuning:
            raise KeyError(
                f"streaming_tuning missing required block: '{block}'"
            )

    # Dedup validation
    if streaming_tuning["dedup"].get("mode", "two_phase") != "none":
        if "buffer_seconds" not in streaming_tuning["dedup"]:
            raise KeyError("dedup.buffer_seconds is required")

    # Sharding validation (only used for two_phase)
    if streaming_tuning["dedup"].get("mode") == "two_phase":
        if "num_shards" not in streaming_tuning["sharding"]:
            raise KeyError("sharding.num_shards is required")

    # Windowing validation (only if enabled)
    if streaming_tuning["windowing"].get("enabled", True):
        if "window_size_sec" not in streaming_tuning["windowing"]:
            raise KeyError("windowing.window_size_sec is required")
    # Expose env (optional, but useful)
    cfg["env"] = (
        pipeline_cfg
        .get("dataflow", {})
        .get("job", {})
        .get("parameters", {})
        .get("env", "dev")
    )

    return cfg
