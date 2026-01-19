#!/bin/bash
set -e

PROJECT_ID="stream-accelerator"
DATASET="analytics"
LOCATION="us-central1"

echo "Setting project: $PROJECT_ID"
gcloud config set project $PROJECT_ID

echo "Checking if dataset exists..."

if bq show --dataset "$PROJECT_ID:$DATASET" >/dev/null 2>&1; then
  echo "✅ Dataset '$DATASET' already exists"
else
  echo "Creating dataset '$DATASET' in location '$LOCATION'..."
  bq mk \
    --dataset \
    --location="$LOCATION" \
    "$PROJECT_ID:$DATASET"
  echo "✅ Dataset created"
fi
