#!/bin/bash
set -e

gcloud config set project stream-accelerator

gcloud services enable \
  dataflow.googleapis.com \
  pubsub.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  logging.googleapis.com
