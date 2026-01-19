import apache_beam as beam
import json
import base64
from apache_beam.io.gcp.pubsub import PubsubMessage

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

            elif isinstance(element, str):
                element_str = element

            elif isinstance(element, dict):
                event = element
                element_str = None

            else:
                raise ValueError(f"Unsupported input type: {type(element)}")

            if element_str is not None:
                try:
                    event = json.loads(element_str)
                except json.JSONDecodeError:
                    event = json.loads(
                        base64.b64decode(element_str).decode("utf-8")
                    )

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

        except Exception as e:
            yield beam.pvalue.TaggedOutput(
                "dlq",
                {
                    "error": f"PreProcess failed: {str(e)}",
                    "raw": str(element)[:500],
                },
            )
