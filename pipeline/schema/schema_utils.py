"""
schema_utils.py

Utilities for runtime schema extraction and schema evolution detection.
This module is independent of Beam and can be unit-tested easily.
"""

from typing import Dict, Any, Set
from datetime import date, datetime, time


# -------------------------------------------------------------------
# Envelope fields (defined by Avro schema)
# -------------------------------------------------------------------
ENVELOPE_FIELDS = [
    "event_id",
    "event_type",
    "event_source",
    "event_ts",
]


def _infer_temporal_string_type(value: str) -> str:
    v = value.strip()
    if not v:
        return "string"

    # RFC3339 timestamp (e.g. 2026-01-14T06:36:15Z / +05:30)
    if "T" in v:
        ts_candidate = v
        if v.endswith("Z"):
            ts_candidate = f"{v[:-1]}+00:00"
        try:
            ts = datetime.fromisoformat(ts_candidate)
            if ts.tzinfo is not None:
                return "timestamp"
            return "datetime"
        except Exception:
            pass

    # DATE: YYYY-MM-DD
    try:
        date.fromisoformat(v)
        return "date"
    except Exception:
        pass

    # TIME: HH:MM[:SS[.ffffff]]
    try:
        time.fromisoformat(v)
        return "time"
    except Exception:
        pass

    return "string"


def _parse_expected_types(expected: str) -> Set[str]:
    raw = str(expected or "").strip().lower()
    if not raw:
        return {"unknown"}

    parts = {p.strip() for p in raw.split("|") if p.strip()}
    if not parts:
        return {"unknown"}
    return parts


# -------------------------------------------------------------------
# Type normalization
# -------------------------------------------------------------------
def normalize_type(value: Any) -> str:
    """
    Convert Python runtime values into stable logical schema types.

    This normalization is REQUIRED so schema comparison is deterministic.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return _infer_temporal_string_type(value)
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "unknown"


# -------------------------------------------------------------------
# Envelope schema extraction
# -------------------------------------------------------------------
def extract_envelope_schema(event: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract schema for envelope-level fields.
    """
    schema: Dict[str, str] = {}

    for field in ENVELOPE_FIELDS:
        if field in event:
            schema[field] = normalize_type(event[field])

    return schema


# -------------------------------------------------------------------
# Payload schema extraction
# -------------------------------------------------------------------
def extract_payload_schema(event: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract schema for payload fields using dotted notation:
    payload.<field>
    """
    schema: Dict[str, str] = {}

    payload = event.get("payload")

    # ParseEvent guarantees payload is dict OR this returns empty
    if not isinstance(payload, dict):
        return schema

    for key, value in payload.items():
        schema[f"payload.{key}"] = normalize_type(value)

    return schema


# -------------------------------------------------------------------
# Full runtime schema extraction (envelope + payload)
# -------------------------------------------------------------------
def extract_runtime_schema(event: Dict[str, Any]) -> Dict[str, str]:
    """
    Build full runtime schema representation for an event.
    """
    schema: Dict[str, str] = {}

    schema.update(extract_envelope_schema(event))
    schema.update(extract_payload_schema(event))

    return schema


# -------------------------------------------------------------------
# Schema diff logic
# -------------------------------------------------------------------
def _types_are_compatible(expected: str, observed: str) -> bool:
    expected_types = _parse_expected_types(expected)

    if observed in expected_types:
        return True

    # Backward-friendly: temporal strings are still valid string payloads.
    if "string" in expected_types and observed in {"date", "time", "datetime", "timestamp"}:
        return True

    return False


def diff_schema(expected: Dict[str, str], observed: Dict[str, str]) -> Dict[str, Any]:
    """
    Compare expected vs observed schema and return structured diff.
    """
    expected_fields = set(expected.keys())
    observed_fields = set(observed.keys())

    new_fields = observed_fields - expected_fields
    missing_fields = expected_fields - observed_fields

    type_changes = {}
    for field in expected_fields & observed_fields:
        if not _types_are_compatible(expected[field], observed[field]):
            type_changes[field] = {
                "expected": expected[field],
                "observed": observed[field],
            }

    return {
        "new_fields": sorted(new_fields),
        "missing_fields": sorted(missing_fields),
        "type_changes": type_changes,
    }


# -------------------------------------------------------------------
# Schema evolution decision helper
# -------------------------------------------------------------------
def has_schema_changed(diff: Dict[str, Any]) -> bool:
    """
    Return True if any schema evolution is detected.
    """
    return bool(
        diff.get("new_fields")
        or diff.get("missing_fields")
        or diff.get("type_changes")
    )
