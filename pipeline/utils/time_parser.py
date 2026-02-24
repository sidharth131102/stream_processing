from datetime import datetime, timezone


def _from_unix_epoch(value: float) -> datetime:
    abs_v = abs(value)

    # Heuristic for unix units:
    # seconds: ~1e9, milliseconds: ~1e12, microseconds: ~1e15
    if abs_v >= 1e14:
        seconds = value / 1_000_000.0
    elif abs_v >= 1e11:
        seconds = value / 1_000.0
    else:
        seconds = value

    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def parse_event_ts_to_utc_datetime(value):
    """
    Parse event_ts from common formats and return timezone-aware UTC datetime.
    Supported inputs:
    - ISO/RFC3339 strings (e.g. 2026-02-23T05:48:10Z)
    - datetime strings like '2026-02-23 05:48:10'
    - unix epoch in seconds/milliseconds/microseconds (number or numeric string)
    """
    if value is None:
        raise ValueError("event_ts is missing")

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _from_unix_epoch(float(value))

    if isinstance(value, str):
        s = value.strip()
        if not s:
            raise ValueError("event_ts is empty")

        # Numeric string epoch.
        try:
            num = float(s)
            return _from_unix_epoch(num)
        except Exception:
            pass

        # RFC3339 / ISO8601 with Z or offset.
        ts_candidate = s
        if ts_candidate.endswith("Z"):
            ts_candidate = f"{ts_candidate[:-1]}+00:00"
        try:
            dt = datetime.fromisoformat(ts_candidate)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass

        # Common fallback formats (assume UTC if timezone missing).
        fallback_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%Y-%m-%d",
            "%Y/%m/%d",
        ]
        for fmt in fallback_formats:
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                continue

    raise ValueError(f"Unsupported event_ts format: {value}")
