from simpleeval import SimpleEval

class BusinessRules:
    def __init__(self, rules):
        self.rules = rules or []
        self.evaluator = SimpleEval()
        self.evaluator.functions = {}   # ❌ no functions
        self.evaluator.names = {}       # populated per-event

    def apply(self, event):
        for rule in self.rules:
            try:
                self.evaluator.names = event
                result = self.evaluator.eval(rule["condition"])
                event[rule["output_field"]] = (
                    rule.get("value", True) if result else False
                )
            except Exception:
                event[rule["output_field"]] = False
        return event
