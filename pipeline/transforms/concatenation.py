class Concatenation:
    def __init__(self, rules):
        self.rules = rules or []

    @staticmethod
    def _get_nested(obj, path):
        keys = path.split(".")
        cur = obj
        for key in keys:
            if not isinstance(cur, dict) or key not in cur:
                return None
            cur = cur[key]
        return cur

    @staticmethod
    def _set_nested(obj, path, value):
        keys = path.split(".")
        cur = obj
        for key in keys[:-1]:
            if key not in cur or not isinstance(cur[key], dict):
                cur[key] = {}
            cur = cur[key]
        cur[keys[-1]] = value

    def apply(self, event):
        if not isinstance(self.rules, list):
            return event

        for rule in self.rules:
            if not isinstance(rule, dict):
                continue

            output_field = rule.get("output_field")
            inputs = rule.get("inputs") or []
            if not output_field or not isinstance(inputs, list):
                continue

            separator = str(rule.get("separator", ""))
            skip_nulls = bool(rule.get("skip_nulls", True))
            default_value = rule.get("default_value", "")
            defaults = rule.get("defaults") or {}

            parts = []
            for input_field in inputs:
                value = self._get_nested(event, input_field)

                if value is None or value == "":
                    value = defaults.get(input_field)

                if value is None or value == "":
                    if skip_nulls:
                        continue
                    value = ""

                parts.append(str(value))

            result = separator.join(parts) if parts else default_value
            self._set_nested(event, output_field, result)

        return event
