import json
import base64
import apache_beam as beam

# ✅ ADD
import logging
from pipeline.observability.metrics import PipelineMetrics

class ParseEvent(beam.DoFn):
        
    def process(self, element):
        try:
            # payload MUST be string
            if not isinstance(element.get("payload"), str):
                raise ValueError("payload is not a string")

            payload_str = element["payload"]
            logging.info(f"Parsing payload for event_id: {element.get('event_id', 'unknown_id')}")
            
            # 🔥 BASE64 FALLBACK LOGIC
            try:
                payload_dict = json.loads(payload_str)
                logging.info("Parsed payload as JSON string")
            except json.JSONDecodeError:
                payload_dict = json.loads(
                    base64.b64decode(payload_str).decode("utf-8")
                )
                logging.info("Parsed payload as base64-encoded JSON string")
            
            element["payload"] = payload_dict
            yield element
            logging.info(f"Successfully parsed event: {element.get('event_id', 'unknown_id')}")

        except Exception as e:
            # ✅ ADD: METRICS
            PipelineMetrics.parse_errors.inc()
            PipelineMetrics.stage_error("parse").inc()

            # ✅ ADD: STRUCTURED LOGGING
            logging.error(json.dumps({
                "severity": "ERROR",
                "stage": "parse",
                "error": str(e),
                "event_id": element.get("event_id"),
            }))

            element["stage"] = "parse"
            element["error"] = f"Payload parsing failed: {e}"
            yield beam.pvalue.TaggedOutput("dlq", element)
