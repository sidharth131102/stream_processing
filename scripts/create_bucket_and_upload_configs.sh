#!/bin/bash
set -e

PROJECT_ID="stream-accelerator"
REGION="us-central1"

# Buckets
CONFIG_BUCKET="stream-accelerator-config"
DATAFLOW_BUCKET="stream-accelerator-dataflow"

echo "Setting project..."
gcloud config set project $PROJECT_ID

# -----------------------------
# Create Config Bucket
# -----------------------------
echo "Creating config bucket..."
gsutil mb -p $PROJECT_ID -l $REGION gs://$CONFIG_BUCKET || true

echo "Uploading config files..."
gsutil cp config/source_mapping.yaml gs://$CONFIG_BUCKET/
gsutil cp config/destination_mapping.yaml gs://$CONFIG_BUCKET/
gsutil cp config/transformation.yaml gs://$CONFIG_BUCKET/
gsutil cp config/validation.yaml gs://$CONFIG_BUCKET/
gsutil cp config/pipeline.yaml gs://$CONFIG_BUCKET/
gsutil cp schemas/json_event_v2.json gs://stream-accelerator-config/schemas/json_event_v2.json

# -----------------------------
# Create Dataflow Bucket
# -----------------------------
echo "Creating Dataflow bucket..."
gsutil mb -p $PROJECT_ID -l $REGION gs://$DATAFLOW_BUCKET || true

echo "Creating Dataflow folders..."
gsutil mkdir gs://stream-accelerator-dataflow/staging || true
gsutil mkdir gs://stream-accelerator-dataflow/temp || true
gsutil mkdir gs://stream-accelerator-dataflow/templates || true


echo "✅ Buckets and configs ready"

