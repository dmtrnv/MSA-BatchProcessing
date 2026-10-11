#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
IMAGE="${IMAGE:-shipment-exporter:0.1.0}"

if ! minikube status >/dev/null 2>&1; then
    echo "Starting MiniKube..."
    minikube start
fi

echo "==> Building $IMAGE (context: $PROJECT_DIR)"
docker build -f "$PROJECT_DIR/docker/Dockerfile" -t "$IMAGE" "$PROJECT_DIR"

echo "==> Loading $IMAGE into MiniKube"
minikube image load "$IMAGE"

echo "==> Image available in MiniKube:"
minikube image ls | grep shipment-exporter || true
