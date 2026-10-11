#!/usr/bin/env bash

set -euo pipefail

NAME="${1:-shipment-export-manual-$(date +%Y%m%d%H%M%S)}"

echo "Creating Job $NAME from CronJob shipment-export"
kubectl -n shipping create job --from=cronjob/shipment-export "$NAME"

echo "Waiting for pod to finish..."
kubectl -n shipping wait --for=condition=complete "job/$NAME" --timeout=900s

echo
echo "==> Pod logs"
kubectl -n shipping logs "job/$NAME" --all-containers=true
