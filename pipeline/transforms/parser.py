# pipeline/transforms/parser.py - FULL FIXED VERSION
import json
import base64
import apache_beam as beam

class ParseEvent(beam.DoFn):
    def __init__(self, source_cfg):
        self.source_cfg = source_cfg
        
    def process(self, element):
        try:
            # payload MUST be string
            if not isinstance(element.get("payload"), str):
                raise ValueError("payload is not a string")

            payload_str = element["payload"]
            
            # 🔥 BASE64 FALLBACK LOGIC
            try:
                payload_dict = json.loads(payload_str)
            except json.JSONDecodeError:
                # Try base64 decode + JSON
                payload_dict = json.loads(base64.b64decode(payload_str).decode('utf-8'))
            
            element["payload"] = payload_dict
            yield element

        except Exception as e:
            element["error"] = f"Payload parsing failed: {e}"
            yield beam.pvalue.TaggedOutput("dlq", element)
