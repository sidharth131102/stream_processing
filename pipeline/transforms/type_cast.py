from datetime import date, datetime, time


class TypeCast:
    def __init__(self, rules):
        self.rules = rules or []

    @staticmethod
    def _get_nested(obj, path):
        keys = (path or "").split(".")
        cur = obj
        for key in keys:
            if not key:
                return None
            if not isinstance(cur, dict) or key not in cur:
                return None
            cur = cur[key]
        return cur

    @staticmethod
    def _set_nested(obj, path, value):
        keys = (path or "").split(".")
        if not keys or not keys[0]:
            return
        cur = obj
        for key in keys[:-1]:
            if key not in cur or not isinstance(cur[key], dict):
                cur[key] = {}
            cur = cur[key]
        cur[keys[-1]] = value

    @staticmethod
    def _to_bool(value):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            v = value.strip().lower()
            if v in {"true", "1", "yes", "y", "t"}:
                return True
            if v in {"false", "0", "no", "n", "f"}:
                return False
        raise ValueError("invalid boolean")

    @staticmethod
    def _to_date(value):
        if isinstance(value, date) and not isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, str):
            return date.fromisoformat(value.strip()).isoformat()
        raise ValueError("invalid date")

    @staticmethod
    def _to_time(value):
        if isinstance(value, time):
            return value.isoformat()
        if isinstance(value, str):
            return time.fromisoformat(value.strip()).isoformat()
        raise ValueError("invalid time")

    @staticmethod
    def _to_datetime(value):
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, str):
            v = value.strip()
            if v.endswith("Z"):
                v = f"{v[:-1]}+00:00"
            return datetime.fromisoformat(v).isoformat()
        raise ValueError("invalid datetime")

    def _cast_value(self, value, to_type):
        t = str(to_type or "").strip().lower()
        if t == "string":
            return str(value)
        if t == "integer":
            if isinstance(value, bool):
                raise ValueError("invalid integer")
            return int(value)
        if t == "float":
            if isinstance(value, bool):
                raise ValueError("invalid float")
            return float(value)
        if t == "boolean":
            return self._to_bool(value)
        if t == "date":
            return self._to_date(value)
        if t == "time":
            return self._to_time(value)
        if t in {"datetime", "timestamp"}:
            return self._to_datetime(value)
        raise ValueError(f"unsupported cast type: {to_type}")

    def apply(self, event):
        if not isinstance(self.rules, list):
            return event

        for rule in self.rules:
            if not isinstance(rule, dict):
                continue

            source_field = rule.get("field")
            target_type = rule.get("to")
            output_field = rule.get("output_field", source_field)
            has_default = "default" in rule
            default = rule.get("default")
            treat_empty_as_null = bool(rule.get("treat_empty_as_null", True))

            if not source_field or not target_type or not output_field:
                continue

            current = self._get_nested(event, source_field)
            if current is None or (treat_empty_as_null and current == ""):
                if has_default:
                    self._set_nested(event, output_field, default)
                continue

            try:
                casted = self._cast_value(current, target_type)
                self._set_nested(event, output_field, casted)
            except Exception:
                if has_default:
                    self._set_nested(event, output_field, default)

        return event
