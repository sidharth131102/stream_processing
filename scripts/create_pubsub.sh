#!/bin/bash
set -e

PROJECT_ID="stream-accelerator"

gcloud pubsub topics create json-events-topic --project $PROJECT_ID || true
gcloud pubsub topics create json-events-dlq --project $PROJECT_ID || true
gcloud pubsub topics create json-events-backfill-dlq --project $PROJECT_ID || true
# gcloud pubsub topics create json-events-backfill-dlq --project stream-accelerator || true
gcloud pubsub subscriptions create json-events-sub \
  --project $PROJECT_ID \
  --topic=json-events-topic \
  --ack-deadline=60 \
  --max-delivery-attempts=5 \
  --dead-letter-topic=json-events-dlq || true
#change hardcoding the topic names

gcloud pubsub subscriptions create json-events-dlq-sub \
  --project $PROJECT_ID \
  --topic=json-events-dlq || true

gcloud pubsub subscriptions create json-events-backfill-dlq-sub \
  --project $PROJECT_ID \
  --topic=json-events-backfill-dlq || true

# gcloud pubsub subscriptions create json-events-backfill-dlq-sub \
#   --project stream-accelerator \
#   --topic=json-events-backfill-dlq || true