import apache_beam as beam
import json
import base64
from apache_beam.io.gcp.pubsub import PubsubMessage

# ✅ ADD
import logging
from pipeline.observability.metrics import PipelineMetrics

REQUIRED_FIELDS = [
    "event_id",
    "event_type",
    "event_source",
    "event_ts",
    "payload",
]

class PreProcess(beam.DoFn):
    def process(self, element):
        try:
            pubsub_meta = None

            if isinstance(element, PubsubMessage):
                element_str = element.data.decode("utf-8")
                pubsub_meta = {
                    "message_id": element.message_id,
                    "attributes": element.attributes,
                    "publish_time": (
                        element.publish_time.isoformat()
                        if element.publish_time
                        else None
                    ),
                }

            elif isinstance(element, bytes):
                element_str = element.decode("utf-8")
                logging.info(f"preprocessing byte elements: {element_str[:100]}")

            elif isinstance(element, str):
                element_str = element
                logging.info(f"preprocessing string element: {element_str[:100]}")

            elif isinstance(element, dict):
                event = element
                element_str = None
                logging.info(f"preprocessing dict element with keys: {list(event.keys())}")

            else:
                raise ValueError(f"Unsupported input type: {type(element)}")

            if element_str is not None:
                try:
                    event = json.loads(element_str)
                    logging.info(f"preprocessed JSON event with keys: {list(event.keys())}")
                except json.JSONDecodeError:
                    event = json.loads(
                        base64.b64decode(element_str).decode("utf-8")
                    )
                    logging.info(f"preprocessed base64-encoded JSON event with keys: {list(event.keys())}")

            if "payload" in event:
                if isinstance(event["payload"], dict):
                    event["payload"] = json.dumps(event["payload"])
                    logging.info("Serialized payload dict to JSON string")
                elif isinstance(event["payload"], str):
                    pass

            if pubsub_meta:
                event["_pubsub"] = pubsub_meta

            for f in REQUIRED_FIELDS:
                if f not in event:
                    raise ValueError(f"Missing required field: {f}")
                if not isinstance(event, dict):
                    raise RuntimeError(
                        f"PreProcess must emit dict, got {type(event)}"
                    )

            yield event
            logging.info(f"Successfully preprocessed event: {event.get('event_id', 'unknown_id')}")

        except Exception as e:
            # ✅ ADD: METRICS
            PipelineMetrics.stage_error("preprocess").inc()
            PipelineMetrics.parse_errors.inc()

            # ✅ ADD: STRUCTURED LOGGING
            logging.error(json.dumps({
                "severity": "ERROR",
                "stage": "preprocess",
                "error": str(e),
                "raw": str(element)[:500],
            }))

            yield beam.pvalue.TaggedOutput(
                "dlq",
                {
                    "stage": "preprocess",
                    "error": f"PreProcess failed: {str(e)}",
                    "raw": str(element)[:500],
                },
            )
