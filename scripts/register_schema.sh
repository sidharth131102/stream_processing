#!/bin/bash
set -e

gcloud pubsub schemas create json_event_v2 \
  --type=avro \
  --definition-file=schemas/json_event_v2.avsc || true

gcloud pubsub topics update json-events-topic \
  --schema=json_event_v2 \
  --message-encoding=json
