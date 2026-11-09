#!/usr/bin/env bash
# Validates k8s/ with `kubectl apply --dry-run` ONLY. kubectl needs an API server for type discovery,
# so this creates a throwaway local kind control plane, runs client + server dry-runs (nothing is
# persisted or scheduled), then deletes the cluster. No workload ever runs.
set -euo pipefail
cd "$(dirname "$0")/.."
KIND="${KIND:-$(command -v kind || echo "$HOME/tools/kind")}"
KUBECTL="${KUBECTL:-$(command -v kubectl || echo "$HOME/tools/kubectl")}"
NAME=tmd-dryrun; OUT=reports/k8s-dry-run.txt; KCFG="$(mktemp)"
trap '"$KIND" delete cluster --name "$NAME" >/dev/null 2>&1 || true; rm -f "$KCFG"' EXIT
"$KIND" create cluster --name "$NAME" --image kindest/node:v1.31.0 --kubeconfig "$KCFG" --wait 60s >/dev/null 2>&1
{
  echo "\$ kubectl version"; "$KUBECTL" --kubeconfig "$KCFG" version 2>&1 | sed 's/^/  /'
  echo "\$ kubectl apply --dry-run=client -f k8s/"; "$KUBECTL" --kubeconfig "$KCFG" apply --dry-run=client -f k8s/
  echo "\$ kubectl apply --dry-run=server -f k8s/namespace.yaml"; "$KUBECTL" --kubeconfig "$KCFG" apply --dry-run=server -f k8s/namespace.yaml
  echo "# server dry-run of namespaced objects needs the namespace to exist; it is created via dry-run only, so"
  echo "# namespaced objects are server-validated with the namespace overridden to 'default':"
  for f in k8s/configmap.yaml k8s/secret.example.yaml k8s/statefulset.yaml k8s/service.yaml k8s/pdb.yaml k8s/networkpolicy.yaml; do
    echo "\$ kubectl apply --dry-run=server -f $f (namespace=default)"
    sed 's/namespace: telemetry-demo/namespace: default/' "$f" | "$KUBECTL" --kubeconfig "$KCFG" apply --dry-run=server -f -
  done
  echo "\$ kubectl get statefulsets,pods -A -l app=rust-processor   # proves nothing was created"
  "$KUBECTL" --kubeconfig "$KCFG" get statefulsets,pods -A -l app=rust-processor 2>&1
} > "$OUT" 2>&1
cat "$OUT"
