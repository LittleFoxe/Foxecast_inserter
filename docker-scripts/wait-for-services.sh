#!/bin/sh
set -eu

check_http() {
  name="$1"
  url="$2"
  max_attempts="${3:-30}"
  sleep_seconds="${4:-2}"
  attempt=1
  while [ "$attempt" -le "$max_attempts" ]; do
    if wget --spider --quiet "$url"; then
      echo "$name is reachable at $url"
      return 0
    fi
    echo "Waiting for $name (attempt $attempt/$max_attempts)..."
    attempt=$((attempt + 1))
    sleep "$sleep_seconds"
  done
  echo "ERROR: $name is unavailable at $url" >&2
  exit 1
}

check_tcp() {
  name="$1"
  host="$2"
  port="$3"
  max_attempts="${4:-30}"
  sleep_seconds="${5:-2}"
  attempt=1
  while [ "$attempt" -le "$max_attempts" ]; do
    if nc -z "$host" "$port"; then
      echo "$name is reachable at $host:$port"
      return 0
    fi
    echo "Waiting for $name (attempt $attempt/$max_attempts)..."
    attempt=$((attempt + 1))
    sleep "$sleep_seconds"
  done
  echo "ERROR: $name is unavailable at $host:$port" >&2
  exit 1
}

clickhouse_url="${CLICKHOUSE_HEALTH_URL:-http://clickhouse:8123/ping}"
minio_url="${MINIO_HEALTH_URL:-http://minio:9000/minio/health/live}"
rabbitmq_host="${RABBITMQ_HOST:-rabbitmq}"
rabbitmq_port="${RABBITMQ_PORT:-5672}"

check_http "ClickHouse" "$clickhouse_url"
check_http "MinIO" "$minio_url"
check_tcp "RabbitMQ" "$rabbitmq_host" "$rabbitmq_port"