class FieldMapper:
    def __init__(self, mapping):
        self.mapping = mapping or {}  # Support empty mapping without errors

    def _get_nested(self, obj, path):
        """Support dot notation: payload.request_id → obj['payload']['request_id']"""
        keys = path.split('.')
        for key in keys:
            if isinstance(obj, dict) and key in obj:
                obj = obj[key]
            else:
                return None
        return obj

    def apply(self, event):
        for target, source in self.mapping.items():
            value = self._get_nested(event, source)
            event[target] = value
        
        # Remove payload after flattening
        if "payload" in event:
            del event["payload"]
        return event
