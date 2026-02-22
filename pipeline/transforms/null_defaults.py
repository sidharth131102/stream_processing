class NullDefaults:
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

    def _iter_rules(self):
        if isinstance(self.rules, dict):
            for field, default in self.rules.items():
                yield {"field": field, "default": default}
            return

        if isinstance(self.rules, list):
            for rule in self.rules:
                if isinstance(rule, dict):
                    yield rule

    def apply(self, event):
        for rule in self._iter_rules():
            field = rule.get("field")
            if not field:
                continue

            default = rule.get("default")
            current = self._get_nested(event, field)

            if current is None or current == "":
                self._set_nested(event, field, default)

        return event
