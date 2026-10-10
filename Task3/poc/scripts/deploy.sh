#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
K8S="$PROJECT_DIR/k8s"

echo "==> Namespace, secrets and config"
kubectl apply -f "$K8S/namespace.yaml"
kubectl apply -f "$K8S/postgres/secret.yaml"
kubectl apply -f "$K8S/exporter/secret-db.yaml"
kubectl apply -f "$K8S/exporter/configmap.yaml"
kubectl apply -f "$K8S/minio/secret.yaml"

echo "==> PostgreSQL"
kubectl apply -f "$K8S/postgres/pvc.yaml"
kubectl apply -f "$K8S/postgres/deployment.yaml"
kubectl apply -f "$K8S/postgres/service.yaml"

echo "==> MinIO"
kubectl apply -f "$K8S/minio/pvc.yaml"
kubectl apply -f "$K8S/minio/deployment.yaml"
kubectl apply -f "$K8S/minio/service.yaml"

echo "==> Waiting for workloads to become ready"
kubectl -n shipping rollout status deployment/postgres --timeout=300s
kubectl -n shipping rollout status deployment/minio --timeout=300s

echo "==> Seeding source data (this can take a couple of minutes)"
kubectl -n shipping delete job postgres-seed --ignore-not-found
kubectl apply -f "$K8S/postgres/seed-job.yaml"
kubectl -n shipping wait --for=condition=complete job/postgres-seed --timeout=1800s

echo "==> Creating export CronJob"
kubectl apply -f "$K8S/exporter/cronjob.yaml"

echo
echo "Done."
