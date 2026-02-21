import json
import uuid
from datetime import datetime, timezone
from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = "stream-accelerator-3"
TOPIC_ID = "json-events-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def publish_messages(count):
    for i in range(15, 5 + count):
        event_id = f"TRANSFORMER-{uuid.uuid4().hex[:8]}"
        ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        
        # Internal payload as a dictionary
        payload_dict = {
            "request_id": f"TFR-{i:03d}",
            "customer_id": f"CUST-{i + 100}",
            "request_type": "POWER_OUTAGE",
            "priority": "CRITICAL",
            "location": "Noida",
            "description": f"Batch test event {i}",
            "source_system": "SmartGridAI"
        }

        # Full event matching your Avro schema (json_event_v2.avsc)
        event_data = {
            "event_id": event_id,
            "event_type": "alert",
            "event_source": "load_test_batch",
            "event_ts": ts,
            "payload": json.dumps(payload_dict) # Stringified JSON as required by schema
        }

        data = json.dumps(event_data).encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(f"Published message {i}: {event_id} (ID: {future.result()})")

if __name__ == "__main__":
    publish_messages(15) # Publish from 5 to 15