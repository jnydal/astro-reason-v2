#!/usr/bin/env bash
# Stop the whole system, then start: db, kafka, grafana, embeddings, resolver, fetch-bio
# Usage: ./scripts/restart-core-pipeline.sh
#
# Services started:
#   - db         : PostgreSQL with pgvector
#   - kafka      : Redpanda Kafka
#   - grafana    : Grafana dashboards
#   - embeddings : Embeddings worker
#   - resolver   : QID resolver
#   - fetch-bio  : Wikipedia bio fetcher

set -e
cd "$(dirname "$0")/.."

echo ""
echo "=== Stopping all services ==="
docker compose down

echo ""
echo "=== Starting core pipeline: db, kafka, grafana, embeddings, resolver, fetch-bio ==="
docker compose up -d db kafka grafana embeddings resolver fetch-bio

echo ""
echo "=== Service status ==="
docker compose ps db kafka grafana embeddings resolver fetch-bio
