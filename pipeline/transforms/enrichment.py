class Enrichment:
    def __init__(self, enrichments):
        self.enrichments = enrichments or []

    def apply(self, event):
        for e in self.enrichments:
            if e["type"] == "static_lookup":
                value = event.get(e["input_field"])
                event[e["output_field"]] = e["lookup"].get(value)
        return event
