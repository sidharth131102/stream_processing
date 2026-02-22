import re
from simpleeval import SimpleEval


class Enrichment:
    def __init__(self, enrichments):
        self.enrichments = enrichments or []
        self.evaluator = SimpleEval()
        self.evaluator.functions = {}

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
    def _as_float(value):
        try:
            return float(value)
        except Exception:
            return None

    def _apply_static_lookup(self, event, cfg):
        value = self._get_nested(event, cfg.get("input_field"))
        lookup = cfg.get("lookup") or {}
        out = lookup.get(value, cfg.get("default"))
        self._set_nested(event, cfg.get("output_field"), out)

    def _apply_range_lookup(self, event, cfg):
        value = self._as_float(self._get_nested(event, cfg.get("input_field")))
        output = cfg.get("default")
        ranges = cfg.get("ranges") or []

        if value is not None:
            for rule in ranges:
                if not isinstance(rule, dict):
                    continue
                min_v = rule.get("min")
                max_v = rule.get("max")
                include_min = rule.get("include_min", True)
                include_max = rule.get("include_max", True)

                lower_ok = True if min_v is None else (
                    value >= min_v if include_min else value > min_v
                )
                upper_ok = True if max_v is None else (
                    value <= max_v if include_max else value < max_v
                )

                if lower_ok and upper_ok:
                    output = rule.get("value")
                    break

        self._set_nested(event, cfg.get("output_field"), output)

    def _apply_multi_key_lookup(self, event, cfg):
        input_fields = cfg.get("input_fields") or []
        lookup = cfg.get("lookup") or {}
        default = cfg.get("default")
        separator = str(cfg.get("separator", "|"))
        normalize_case = bool(cfg.get("normalize_case", False))

        parts = []
        for field in input_fields:
            val = self._get_nested(event, field)
            if val is None:
                parts.append("")
            else:
                text = str(val).strip()
                if normalize_case:
                    text = text.lower()
                parts.append(text)
        key = separator.join(parts)
        self._set_nested(event, cfg.get("output_field"), lookup.get(key, default))

    def _apply_regex_lookup(self, event, cfg):
        text = self._get_nested(event, cfg.get("input_field"))
        default = cfg.get("default")
        output = default
        patterns = cfg.get("patterns") or []

        if isinstance(text, str):
            for p in patterns:
                if not isinstance(p, dict):
                    continue
                pattern = p.get("pattern")
                if not pattern:
                    continue

                flags = 0
                if p.get("ignore_case", True):
                    flags |= re.IGNORECASE

                match = re.search(pattern, text, flags)
                if match:
                    output = p.get("value")
                    if cfg.get("first_match", True):
                        break

        self._set_nested(event, cfg.get("output_field"), output)

    def _apply_conditional_set(self, event, cfg):
        output = cfg.get("default")
        conditions = cfg.get("conditions") or []

        for c in conditions:
            if not isinstance(c, dict):
                continue
            expr = c.get("when")
            if not expr:
                continue
            try:
                self.evaluator.names = {"event": event, **event}
                if self.evaluator.eval(expr):
                    output = c.get("value")
                    break
            except Exception:
                continue

        self._set_nested(event, cfg.get("output_field"), output)

    def _apply_derived_fields(self, event, cfg):
        fields = cfg.get("fields")
        if not isinstance(fields, list):
            fields = [cfg]

        for item in fields:
            if not isinstance(item, dict):
                continue
            out_field = item.get("output_field")
            expr = item.get("expression")
            if not out_field or not expr:
                continue
            try:
                self.evaluator.names = {"event": event, **event}
                value = self.evaluator.eval(expr)
            except Exception:
                value = item.get("default")
            self._set_nested(event, out_field, value)

    def _apply_normalization(self, event, cfg):
        input_field = cfg.get("input_field")
        output_field = cfg.get("output_field", input_field)
        value = self._get_nested(event, input_field)
        if value is None:
            self._set_nested(event, output_field, cfg.get("default"))
            return

        text = str(value)
        ops = cfg.get("operations") or []

        for op in ops:
            if op == "trim":
                text = text.strip()
            elif op == "lower":
                text = text.lower()
            elif op == "upper":
                text = text.upper()
            elif op == "title":
                text = text.title()
            elif op == "collapse_spaces":
                text = " ".join(text.split())

        synonyms = cfg.get("synonyms") or {}
        text = synonyms.get(text, text)
        self._set_nested(event, output_field, text)

    def apply(self, event):
        for e in self.enrichments:
            etype = e.get("type")
            if etype == "static_lookup":
                self._apply_static_lookup(event, e)
            elif etype == "range_lookup":
                self._apply_range_lookup(event, e)
            elif etype == "multi_key_lookup":
                self._apply_multi_key_lookup(event, e)
            elif etype == "regex_lookup":
                self._apply_regex_lookup(event, e)
            elif etype == "conditional_set":
                self._apply_conditional_set(event, e)
            elif etype == "derived_fields":
                self._apply_derived_fields(event, e)
            elif etype == "normalization_enrichment":
                self._apply_normalization(event, e)
        return event
