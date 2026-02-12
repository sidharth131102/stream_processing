import re

class EntityExtractor:
    def __init__(self, configs):
        self.configs = configs or []

    def apply(self, event):
        for cfg in self.configs:
            text = event.get(cfg["field"], "")
            # ✅ FIX: Handle None/empty text BEFORE entity loop
            if not isinstance(text, str) or not text.strip():
                continue  # Skip if no text
                
            for ent in cfg["entities"]:  # ✅ ent defined HERE
                match = re.search(ent["pattern"], text, re.IGNORECASE)
                event[ent["name"]] = match.group(0) if match else None  # ✅ ent safe
        return event
