#!/usr/bin/env sh
set -eu

bootstrap_file() {
  target="$1"
  example="$2"

  if [ ! -f "$target" ] && [ -f "$example" ]; then
    cp "$example" "$target"
    echo "Bootstrapped $target from $example"
  fi
}

mkdir -p /app/configs /app/logs /app/exports
mkdir -p /app/configs/calendar/2025 /app/configs/calendar/2026

bootstrap_file /app/configs/.env /app/configs/.env.example
bootstrap_file /app/configs/cluster.yaml /app/configs/cluster.example.yaml
bootstrap_file /app/configs/scheduler.yaml /app/configs/scheduler.example.yaml
bootstrap_file /app/configs/server.yaml /app/configs/server.example.yaml

exec /app/taskshift "$@"
